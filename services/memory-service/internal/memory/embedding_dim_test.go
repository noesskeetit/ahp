package memory

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// dimTestPool builds a pool that never dials: pgx connects lazily, so the
// wiring rules below are testable with no database in sight.
func dimTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://ahp_invalid:invalid@127.0.0.1:1/ahp_invalid")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

var dimTestAuth Authorize = func(context.Context, contracts.Scope, string, string) error { return nil }

// declaredSource is an embedder that reports its own width, the way a real
// backend does from its configuration.
type declaredSource struct {
	width int
	err   error
}

func (d declaredSource) Embed(context.Context, string) ([]float32, error) {
	if d.err != nil {
		return nil, d.err
	}
	return make([]float32, d.width), nil
}
func (d declaredSource) Dimensions() int { return d.width }

// A production embedder is 384/1024/1536 wide, never 64. The pgvector column
// width is therefore a property of the wiring, not a compile-time constant;
// without this the honest embedder could only be mounted by lying about it.
func TestEmbeddingWidthIsConfiguredNotConst(t *testing.T) {
	s, err := New(dimTestPool(t), nil, dimTestAuth, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.EmbeddingWidth(); got != EmbeddingDim {
		t.Fatalf("default width %d, must stay %d for accepted schemas", got, EmbeddingDim)
	}
	if !strings.Contains(ddlFor(s.EmbeddingWidth()), "vector(64)") {
		t.Fatal("the default DDL must keep the accepted column width")
	}

	s2, err := New(dimTestPool(t), nil, dimTestAuth, nil, WithEmbedding(declaredSource{width: 384}))
	if err != nil {
		t.Fatal(err)
	}
	if s2.EmbeddingWidth() != 384 {
		t.Fatalf("width %d, want 384", s2.EmbeddingWidth())
	}
	if !strings.Contains(ddlFor(s2.EmbeddingWidth()), "vector(384)") {
		t.Fatal("the column must be created at the wired embedder's width")
	}
}

// The width comes from the embedder itself, so the pair cannot drift apart.
func TestEmbeddingWidthComesFromTheEmbedder(t *testing.T) {
	src := declaredSource{width: 384}
	s, err := New(dimTestPool(t), nil, dimTestAuth, nil, WithEmbedding(src))
	if err != nil {
		t.Fatal(err)
	}
	if s.embedder == nil {
		t.Fatal("the embedder was not installed")
	}
	vec, err := s.embedder(context.Background(), "x")
	if err != nil || len(vec) != s.EmbeddingWidth() {
		t.Fatalf("embedder/column disagreement: %d vs %d (%v)", len(vec), s.EmbeddingWidth(), err)
	}
}

func TestEmbeddingWidthBounds(t *testing.T) {
	for _, d := range []int{0, -1, 20000} {
		if _, err := New(dimTestPool(t), nil, dimTestAuth, nil, WithEmbedding(declaredSource{width: d})); err == nil {
			t.Fatalf("width %d must be refused", d)
		} else if !strings.Contains(err.Error(), "dimensions") {
			t.Fatalf("the refusal must name the dimension, got %q", err)
		}
	}
}

func TestEmbeddingSourceFailurePropagates(t *testing.T) {
	sentinel := contracts.NewError(contracts.ErrUnavailable, "memory/embed: upstream 503")
	s, err := New(dimTestPool(t), nil, dimTestAuth, nil, WithEmbedding(declaredSource{width: 4, err: sentinel}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.embedder(context.Background(), "x"); !errors.Is(err, sentinel) {
		t.Fatalf("the embedder's own failure must reach the caller, got %v", err)
	}
}

// A wrong-width answer used to be skipped in silence: the write stored NULL and
// search reported results as though the vector signal had run. Both paths must
// name the contradiction instead.
func TestWrongWidthEmbeddingIsANamedFailure(t *testing.T) {
	s, err := New(dimTestPool(t), nil, dimTestAuth, nil, WithEmbedding(declaredSource{width: 4}))
	if err != nil {
		t.Fatal(err)
	}
	err = s.checkEmbedding([]float32{1, 2, 3})
	if err == nil {
		t.Fatal("a 3-wide vector in a 4-wide index must not be accepted")
	}
	var e *contracts.Error
	if !errors.As(err, &e) || e.Code != contracts.ErrUnavailable {
		t.Fatalf("a lying embedder is a dependency failure, not the caller's: %v", err)
	}
	if !strings.Contains(err.Error(), "3") || !strings.Contains(err.Error(), "4") {
		t.Fatalf("the refusal must name both widths, got %q", err)
	}
	if err := s.checkEmbedding([]float32{1, 2, 3, 4}); err != nil {
		t.Fatalf("the correct width must pass, got %v", err)
	}
}
