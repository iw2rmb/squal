package complete

import (
	"testing"

	"github.com/iw2rmb/squall/core"
)

func TestCatalogInit(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: healthyParserStub()})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}
	if version == "" {
		t.Fatal("InitCatalog() returned empty catalog version")
	}

	resp, err := engine.Complete(Request{
		SQL:            "select 1",
		CursorByte:     0,
		CatalogVersion: version,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(resp.Diagnostics) != 0 {
		t.Fatalf("Complete() diagnostics = %#v, want none", resp.Diagnostics)
	}
}

func TestCatalogUpdate(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: healthyParserStub()})

	initialVersion, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	equivalentVersion, err := engine.UpdateCatalog(catalogSnapshotVariantB())
	if err != nil {
		t.Fatalf("UpdateCatalog(equivalent) error = %v", err)
	}
	if initialVersion != equivalentVersion {
		t.Fatalf("equivalent snapshots should hash to same version: %q vs %q", initialVersion, equivalentVersion)
	}

	changedVersion, err := engine.UpdateCatalog(catalogSnapshotChanged())
	if err != nil {
		t.Fatalf("UpdateCatalog(changed) error = %v", err)
	}
	if changedVersion == equivalentVersion {
		t.Fatalf("changed snapshot should hash to different version: %q", changedVersion)
	}

	resp, err := engine.Complete(Request{
		SQL:            "select * from public.orders",
		CursorByte:     0,
		CatalogVersion: changedVersion,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(resp.Diagnostics) != 0 {
		t.Fatalf("Complete() diagnostics = %#v, want none", resp.Diagnostics)
	}
}

func TestCatalogVersionDeterminism(t *testing.T) {
	t.Parallel()

	engineA := NewEngine(Config{Parser: healthyParserStub()})
	engineB := NewEngine(Config{Parser: healthyParserStub()})

	versionA, err := engineA.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("engineA.InitCatalog() error = %v", err)
	}
	versionB, err := engineB.InitCatalog(catalogSnapshotVariantB())
	if err != nil {
		t.Fatalf("engineB.InitCatalog() error = %v", err)
	}
	if versionA != versionB {
		t.Fatalf("equivalent snapshots should be deterministic: %q vs %q", versionA, versionB)
	}
}

func TestCatalogVersionUnknown(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: healthyParserStub()})
	resp, err := engine.Complete(Request{
		SQL:            "select 1",
		CursorByte:     0,
		CatalogVersion: CatalogVersion("sha256:missing"),
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(resp.Candidates) != 0 {
		t.Fatalf("Complete() candidates = %d, want 0", len(resp.Candidates))
	}
	if len(resp.Diagnostics) != 1 {
		t.Fatalf("Complete() diagnostics = %#v, want exactly one", resp.Diagnostics)
	}
	if resp.Diagnostics[0].Code != CatalogVersionUnknown {
		t.Fatalf("Complete() diagnostic code = %q, want %q", resp.Diagnostics[0].Code, CatalogVersionUnknown)
	}
}

func catalogSnapshotVariantA() CatalogSnapshot {
	return CatalogSnapshot{
		Schemas: []core.CatalogSchema{
			{
				Name: "public",
				Tables: []core.CatalogTable{
					{
						Schema: "public",
						Name:   "orders",
						Columns: []core.CatalogColumn{
							{Name: "total", Type: "numeric", Nullable: false},
							{Name: "customer_id", Type: "bigint", Nullable: false},
							{Name: "id", Type: "bigint", Nullable: false},
						},
						PrimaryKey: []string{"id"},
						ForeignKeys: []core.CatalogForeignKey{
							{
								Name:       "orders_customer_fk",
								Columns:    []string{"customer_id"},
								RefSchema:  "public",
								RefTable:   "customers",
								RefColumns: []string{"id"},
							},
						},
					},
					{
						Schema: "public",
						Name:   "customers",
						Columns: []core.CatalogColumn{
							{Name: "id", Type: "bigint", Nullable: false},
							{Name: "email", Type: "text", Nullable: false},
						},
						PrimaryKey: []string{"id"},
					},
				},
			},
			{
				Name: "analytics",
				Tables: []core.CatalogTable{
					{
						Schema: "analytics",
						Name:   "events",
						Columns: []core.CatalogColumn{
							{Name: "ts", Type: "timestamptz", Nullable: false},
							{Name: "event_id", Type: "uuid", Nullable: false},
						},
					},
				},
			},
		},
		SearchPath: []string{"public", "analytics"},
	}
}

func catalogSnapshotVariantB() CatalogSnapshot {
	return CatalogSnapshot{
		Schemas: []core.CatalogSchema{
			{
				Name: "analytics",
				Tables: []core.CatalogTable{
					{
						Schema: "analytics",
						Name:   "events",
						Columns: []core.CatalogColumn{
							{Name: "event_id", Type: "uuid", Nullable: false},
							{Name: "ts", Type: "timestamptz", Nullable: false},
						},
					},
				},
			},
			{
				Name: "public",
				Tables: []core.CatalogTable{
					{
						Schema: "public",
						Name:   "customers",
						Columns: []core.CatalogColumn{
							{Name: "email", Type: "text", Nullable: false},
							{Name: "id", Type: "bigint", Nullable: false},
						},
						PrimaryKey: []string{"id"},
					},
					{
						Schema: "public",
						Name:   "orders",
						Columns: []core.CatalogColumn{
							{Name: "id", Type: "bigint", Nullable: false},
							{Name: "customer_id", Type: "bigint", Nullable: false},
							{Name: "total", Type: "numeric", Nullable: false},
						},
						PrimaryKey: []string{"id"},
						ForeignKeys: []core.CatalogForeignKey{
							{
								Name:       "orders_customer_fk",
								Columns:    []string{"customer_id"},
								RefSchema:  "public",
								RefTable:   "customers",
								RefColumns: []string{"id"},
							},
						},
					},
				},
			},
		},
		SearchPath: []string{"public", "analytics"},
	}
}

func catalogSnapshotChanged() CatalogSnapshot {
	snapshot := catalogSnapshotVariantA()
	snapshot.Schemas[0].Tables[0].Columns = append(snapshot.Schemas[0].Tables[0].Columns, core.CatalogColumn{
		Name:     "status",
		Type:     "text",
		Nullable: false,
	})
	return snapshot
}
