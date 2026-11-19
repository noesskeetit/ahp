package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func compactStateClaimsAuthorize(_ context.Context, claims auth.Claims, capability string) error {
	if claims.Kind != auth.KindTask || !slices.Contains(claims.Capabilities, capability) {
		return contracts.NewError(contracts.ErrForbidden, "task token does not grant the requested State capability")
	}
	return nil
}

func mountCompactStateRoutes(mux *http.ServeMux, handler http.Handler) {
	for _, path := range []string{
		"/v1/state/sessions", "/v1/state/sessions/", "/v1/state/sessions:acquire",
		"/v1/state/sessions:enqueue", "/v1/state/sessions:claim-turn", "/v1/state/kv",
		"/v1/state/kv:mutate", "/v1/state/aux:mutate", "/v1/state/intents:mutate",
	} {
		mux.Handle(path, handler)
	}
}

func (s *server) mountCompactState(ctx context.Context, mux *http.ServeMux, verifier *auth.Verifier,
	authorize transport.Authorize) (func(), error) {
	if envOr("AHP_ENABLE_COMPACT_STATE", "") != "1" {
		return func() {}, nil
	}
	if s.tlsCfg == nil || s.tlsCfg.ClientCAs == nil ||
		(s.tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert && s.tlsCfg.ClientAuth != tls.VerifyClientCertIfGiven) {
		return nil, errors.New("compact State requires the State TLS key pair and workload CA")
	}
	if s.pool == nil {
		return nil, errors.New("compact State requires PostgreSQL")
	}

	s3, err := compactStateS3Settings(os.Getenv)
	if err != nil {
		return nil, err
	}
	retention, err := compactStateDuration("AHP_STATE_RETENTION", 30*24*time.Hour, time.Second)
	if err != nil {
		return nil, err
	}
	staging, err := compactStateDuration("AHP_STATE_STAGING_TTL", time.Hour, 30*time.Second)
	if err != nil {
		return nil, err
	}
	signer, err := compactStateToolSigner(os.Getenv)
	if err != nil {
		return nil, err
	}

	startup, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	store := statejournal.New(s.pool)
	var ready func(context.Context) error
	if s3.endpoint != "" {
		client, err := minio.New(s3.endpoint, &minio.Options{
			Secure: s3.secure, Region: s3.region,
			Creds: credentials.NewStaticV4(s3.accessKey, s3.secretKey, ""),
		})
		if err != nil {
			return nil, errors.New("compact State could not configure the S3 client")
		}
		store, err = statejournal.NewWithS3(startup, s.pool, client, s3.bucket)
		if err != nil {
			return nil, err
		}
		ready = func(ctx context.Context) error {
			if exists, err := client.BucketExists(ctx, s3.bucket); err != nil || !exists {
				return errors.New("compact State S3 bucket is unavailable")
			}
			return nil
		}
	}

	var handler http.Handler
	if signer != nil {
		handler, err = statejournal.NewHTTPWithToolLeases(store, verifier, authorize, signer)
	} else {
		handler, err = statejournal.NewHTTP(store, verifier, authorize)
	}
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(startup); err != nil {
		return nil, fmt.Errorf("compact State migration: %w", err)
	}
	mountCompactStateRoutes(mux, handler)
	s.compactStateReady = ready
	s.logger.Info("compact State mounted", "s3", s3.endpoint != "", "tool_lease_proof", signer != nil)

	run, stop := context.WithCancel(ctx)
	var workers sync.WaitGroup
	workers.Go(func() {
		runCompactStateSweep(run, 2*time.Second, s.logger, "expired turns", func(tick context.Context) error {
			tenants, err := store.FindExpiredTurnTenants(tick)
			if err != nil {
				return err
			}
			var result error
			for _, tenant := range tenants {
				_, err := store.RequeueExpiredTurns(tick, tenant, 64)
				result = errors.Join(result, err)
			}
			return result
		})
	})
	if s3.endpoint != "" {
		workers.Go(func() {
			runCompactStateSweep(run, 45*time.Second, s.logger, "retention", func(tick context.Context) error {
				now := time.Now()
				deletedBefore, stagedBefore := now.Add(-retention), now.Add(-staging)
				tenants, err := store.FindMaintenanceTenants(tick, deletedBefore, stagedBefore)
				if err != nil {
					return err
				}
				var result error
				for _, tenant := range tenants {
					result = errors.Join(result, store.SweepDeletedSessions(tick, tenant, deletedBefore, 64))
					result = errors.Join(result, store.SweepOrphanBlobs(tick, tenant, stagedBefore))
				}
				return result
			})
		})
	}
	return func() {
		stop()
		workers.Wait()
	}, nil
}

func runCompactStateSweep(ctx context.Context, interval time.Duration, log *slog.Logger,
	name string, sweep func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick, cancel := context.WithTimeout(ctx, interval)
			err := sweep(tick)
			cancel()
			if err != nil && ctx.Err() == nil {
				log.Warn("compact State maintenance will retry", "sweep", name, "error", err)
			}
		}
	}
}

type compactStateS3Config struct {
	endpoint, bucket, region, accessKey, secretKey string
	secure                                         bool
}

func compactStateS3Settings(env func(string) string) (compactStateS3Config, error) {
	config := compactStateS3Config{region: env("AHP_S3_REGION"), bucket: env("AHP_STATE_S3_BUCKET")}
	if endpoint := env("AHP_STATE_S3_ENDPOINT"); endpoint != "" {
		config.endpoint = endpoint
		config.secure = true
		config.accessKey = env("AHP_STATE_S3_ACCESS_KEY")
		config.secretKey = env("AHP_STATE_S3_SECRET_KEY")
		if raw := env("AHP_STATE_S3_TLS"); raw != "" {
			secure, err := strconv.ParseBool(raw)
			if err != nil {
				return config, errors.New("AHP_STATE_S3_TLS must be a boolean")
			}
			config.secure = secure
		}
	} else {
		raw := env("AHP_S3_ENDPOINT")
		if raw == "" {
			if config.bucket != "" || env("AHP_STATE_S3_ACCESS_KEY") != "" ||
				env("AHP_STATE_S3_SECRET_KEY") != "" || env("AHP_STATE_S3_TLS") != "" {
				return config, errors.New("compact State S3 settings require an endpoint")
			}
			return config, nil
		}
		endpoint, err := url.Parse(raw)
		if err != nil || endpoint.Hostname() == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") ||
			endpoint.User != nil || (endpoint.Path != "" && endpoint.Path != "/") ||
			endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" {
			return config, errors.New("compact State requires a bare HTTP(S) AHP_S3_ENDPOINT origin")
		}
		if env("AHP_STATE_S3_ACCESS_KEY") != "" || env("AHP_STATE_S3_SECRET_KEY") != "" || env("AHP_STATE_S3_TLS") != "" {
			return config, errors.New("compact State S3 credentials and TLS settings require AHP_STATE_S3_ENDPOINT")
		}
		config.endpoint = endpoint.Host
		config.secure = endpoint.Scheme == "https"
		config.accessKey = env("AHP_MINIO_ROOT_USER")
		config.secretKey = env("AHP_MINIO_ROOT_PASSWORD")
		if config.bucket == "" {
			config.bucket = env("AHP_S3_BUCKET")
		}
	}
	if config.bucket == "" || config.accessKey == "" || config.secretKey == "" {
		return config, errors.New("compact State S3 requires an existing bucket and both credentials")
	}
	return config, nil
}

func compactStateDuration(key string, fallback, minimum time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < minimum {
		return 0, fmt.Errorf("%s must be a duration of at least %s", key, minimum)
	}
	return duration, nil
}

func compactStateToolSigner(env func(string) string) (*auth.Signer, error) {
	path := env("AHP_STATE_TOOL_LEASE_KEY")
	issuer := env("AHP_STATE_TOOL_LEASE_ISSUER")
	kid := env("AHP_STATE_TOOL_LEASE_KID")
	if path == "" && issuer == "" && kid == "" {
		return nil, nil
	}
	if path == "" || issuer == "" || kid == "" {
		return nil, errors.New("compact State Tool Gateway proof requires key, issuer and kid")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("compact State could not read the tool lease signing key")
	}
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("compact State tool lease key must be PKCS#8 Ed25519 PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	private, ok := key.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("compact State tool lease key must be PKCS#8 Ed25519 PEM")
	}
	return auth.NewSigner(issuer, kid, private)
}
