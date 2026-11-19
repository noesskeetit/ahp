// synthetic-authority is an explicit local PoC issuer/token authority. It is
// not Cloud IAM. There is no public registration or arbitrary-subject mint API.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type destination struct {
	Audience string `json:"audience"`
	Resource string `json:"resource"`
	Scope    string `json:"scope"`
}
type config struct {
	Profile         string                 `json:"profile"`
	Issuer          string                 `json:"issuer"`
	Audience        string                 `json:"audience"`
	AHPBearerIssuer string                 `json:"ahp_issuer"`
	AHPKeys         map[string]string      `json:"ahp_public_key_files"`
	Connections     map[string]destination `json:"connections"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "synthetic authority:", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: synthetic-authority init|serve|mint --dir PRIVATE_DIRECTORY")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	dir := fs.String("dir", "", "private authority directory")
	issuer := fs.String("issuer", "https://127.0.0.1:29443", "synthetic issuer URL")
	audience := fs.String("audience", "ahp-public", "public access assertion audience")
	pubfile := fs.String("ahp-public-key", "", "trusted AHP Ed25519 public key PEM")
	kid := fs.String("ahp-kid", "dev-1", "trusted AHP key identifier")
	tenant := fs.String("tenant", "", "operator-selected tenant")
	subject := fs.String("subject", "", "operator-selected subject")
	out := fs.String("out", "", "new private token output file")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("--dir is required")
	}
	if args[0] == "init" {
		return initialize(*dir, *issuer, *audience, *kid, *pubfile)
	}
	cfg, key, keys, err := load(*dir)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	dsn := os.Getenv("AHP_DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("AHP_DATABASE_URL must identify this synthetic platform's Identity database")
	}
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	pc.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return fmt.Errorf("database unavailable")
	}
	defer pool.Close()
	ids := identity.New(pool, identity.WithResourceEvaluator(identity.NewSyntheticResourcePolicy(pool)))
	if err = ids.Migrate(ctx); err != nil {
		return fmt.Errorf("Identity migration failed")
	}
	switch args[0] {
	case "mint":
		if *out == "" {
			return fmt.Errorf("--out is required; tokens are never printed")
		}
		scope := contracts.Scope{Tenant: *tenant, Subject: *subject, Deployment: "public"}
		if err = ids.RequireResource(ctx, identity.ResourceRequest{Scope: scope, Resource: identity.Resource{Kind: "tenant", ID: *tenant}, Action: "login"}); err != nil {
			return fmt.Errorf("operator principal is not authorized by synthetic IAM")
		}
		signer, e := auth.NewSigner(cfg.Issuer, "synthetic-authority", key)
		if e != nil {
			return e
		}
		token, e := signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: cfg.Audience, Tenant: *tenant, Subject: *subject}, 5*time.Minute)
		if e != nil {
			return e
		}
		if e = privateWrite(*out, []byte(token+"\n")); e != nil {
			return e
		}
		fmt.Println("synthetic access assertion written to", *out)
		return nil
	case "serve":
		u, _ := url.Parse(cfg.Issuer)
		srv := &http.Server{Addr: u.Host, Handler: newAuthority(cfg, key, keys, ids), TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
		go func() {
			<-ctx.Done()
			c, done := context.WithTimeout(context.Background(), 3*time.Second)
			defer done()
			_ = srv.Shutdown(c)
		}()
		fmt.Println("SYNTHETIC authority listening on", cfg.Issuer)
		err = srv.ListenAndServeTLS(filepath.Join(*dir, "tls.pem"), filepath.Join(*dir, "tls-key.pem"))
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("synthetic HTTPS listener stopped")
	default:
		return fmt.Errorf("unknown command")
	}
}

func initialize(dir, issuer, audience, kid, pubfile string) error {
	u, e := url.Parse(issuer)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return fmt.Errorf("issuer must be an explicit HTTPS origin")
	}
	if ip := net.ParseIP(u.Hostname()); ip == nil || !ip.IsLoopback() || u.Port() == "" {
		return fmt.Errorf("synthetic issuer needs a literal loopback IP and port")
	}
	if audience == "" || kid == "" || pubfile == "" {
		return fmt.Errorf("audience and trusted --ahp-public-key / --ahp-kid are required")
	}
	pubfile, e = filepath.Abs(pubfile)
	if e != nil {
		return e
	}
	if _, e = readPublic(pubfile); e != nil {
		return e
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	if e = os.Chmod(dir, 0700); e != nil {
		return e
	}
	_, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		return e
	}
	der, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		return e
	}
	if e = privateWrite(filepath.Join(dir, "issuer-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})); e != nil {
		return e
	}
	pub, tlsKey, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		return e
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "AHP SYNTHETIC authority"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(7 * 24 * time.Hour), IPAddresses: []net.IP{net.ParseIP(u.Hostname())}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	cert, e := x509.CreateCertificate(rand.Reader, template, template, pub, tlsKey)
	if e != nil {
		return e
	}
	der, e = x509.MarshalPKCS8PrivateKey(tlsKey)
	if e != nil {
		return e
	}
	if e = privateWrite(filepath.Join(dir, "tls-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})); e != nil {
		return e
	}
	if e = privateWrite(filepath.Join(dir, "tls.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})); e != nil {
		return e
	}
	cfg := config{Profile: "synthetic", Issuer: issuer, Audience: audience, AHPBearerIssuer: "ahp", AHPKeys: map[string]string{kid: pubfile}, Connections: map[string]destination{}}
	raw, _ := json.MarshalIndent(cfg, "", "  ")
	if e = privateWrite(filepath.Join(dir, "config.json"), append(raw, '\n')); e != nil {
		return e
	}
	fmt.Println("synthetic authority initialized in", dir)
	return nil
}

func privateWrite(path string, data []byte) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return fmt.Errorf("private output must be a new writable file")
	}
	_, e = f.Write(data)
	ce := f.Close()
	if e != nil {
		return fmt.Errorf("private output write failed")
	}
	return ce
}
func readPrivate(path string) ([]byte, error) {
	i, e := os.Lstat(path)
	if e != nil || !i.Mode().IsRegular() || i.Mode().Perm() != 0600 {
		return nil, fmt.Errorf("private authority file must be regular with mode 0600")
	}
	return os.ReadFile(path)
}
func readPublic(path string) (ed25519.PublicKey, error) {
	raw, e := os.ReadFile(path)
	if e != nil {
		return nil, fmt.Errorf("trusted public key unavailable")
	}
	p, _ := pem.Decode(raw)
	if p == nil {
		return nil, fmt.Errorf("invalid trusted public key")
	}
	k, e := x509.ParsePKIXPublicKey(p.Bytes)
	if e != nil {
		return nil, fmt.Errorf("invalid trusted public key")
	}
	key, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("trusted key must be Ed25519")
	}
	return key, nil
}
func load(dir string) (config, ed25519.PrivateKey, map[string]ed25519.PublicKey, error) {
	var cfg config
	raw, e := readPrivate(filepath.Join(dir, "config.json"))
	if e != nil {
		return cfg, nil, nil, e
	}
	if e = json.Unmarshal(raw, &cfg); e != nil {
		return cfg, nil, nil, fmt.Errorf("invalid config")
	}
	u, e := url.Parse(cfg.Issuer)
	if e != nil || u.Scheme != "https" || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() || u.Port() == "" || u.Path != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || cfg.Profile != "synthetic" || cfg.Audience == "" || cfg.AHPBearerIssuer == "" || len(cfg.AHPKeys) == 0 {
		return cfg, nil, nil, fmt.Errorf("invalid synthetic authority configuration")
	}
	raw, e = readPrivate(filepath.Join(dir, "issuer-key.pem"))
	if e != nil {
		return cfg, nil, nil, e
	}
	p, _ := pem.Decode(raw)
	if p == nil {
		return cfg, nil, nil, fmt.Errorf("invalid authority key")
	}
	k, e := x509.ParsePKCS8PrivateKey(p.Bytes)
	if e != nil {
		return cfg, nil, nil, fmt.Errorf("invalid authority key")
	}
	key, ok := k.(ed25519.PrivateKey)
	if !ok {
		return cfg, nil, nil, fmt.Errorf("invalid authority key type")
	}
	if _, e = readPrivate(filepath.Join(dir, "tls-key.pem")); e != nil {
		return cfg, nil, nil, e
	}
	keys := map[string]ed25519.PublicKey{}
	for kid, path := range cfg.AHPKeys {
		pub, e := readPublic(path)
		if e != nil {
			return cfg, nil, nil, e
		}
		keys[kid] = pub
	}
	for name, d := range cfg.Connections {
		if name == "" || d.Audience == "" || d.Resource == "" || d.Scope == "" {
			return cfg, nil, nil, fmt.Errorf("incomplete trusted MCP registration")
		}
	}
	return cfg, key, keys, nil
}
