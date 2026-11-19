// Root stand initializer writes only the explicitly named isolated DB's model
// label and quota records through their owning stores.
package main

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "stand init failed (credentials omitted)")
		os.Exit(1)
	}
}
func run() error {
	dsn := os.Getenv("AHP_DATABASE_URL")
	u, err := url.Parse(dsn)
	if err != nil || u.Path != "/ahp_root_stand_20261102" {
		return fmt.Errorf("wrong database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer p.Close()
	store := model.NewDeploymentLabelStore(p)
	if err = store.Migrate(ctx); err != nil {
		return err
	}
	for _, deployment := range []string{"djrun", "space", "custom-a", "custom-b", "custom-a2", "custom-b2", "custom-a3", "custom-b3", "custom-a4", "custom-b4"} {
		if err = store.Set(ctx, "rootstand", deployment, labels.Set{}, "operator:root-final-stand-reference-workloads"); err != nil {
			return err
		}
	}
	quota := model.NewQuotaStore(p)
	if err = quota.Migrate(ctx); err != nil {
		return err
	}
	if err = quota.Set(ctx, model.ProviderQuota{Provider: "modelrun", Tenant: "", RPM: 20, TPM: 120000, WindowSeconds: 60}); err != nil {
		return err
	}
	fmt.Println("isolated stand deployment label records and bounded provider quota initialized")
	return nil
}
