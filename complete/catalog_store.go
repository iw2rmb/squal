package complete

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"

	"github.com/iw2rmb/squal/core"
)

type catalogStore struct {
	mu       sync.RWMutex
	versions map[CatalogVersion]CatalogSnapshot
}

func newCatalogStore() *catalogStore {
	return &catalogStore{
		versions: make(map[CatalogVersion]CatalogSnapshot),
	}
}

func (s *catalogStore) put(snapshot CatalogSnapshot) (CatalogVersion, error) {
	canonical := canonicalizeCatalogSnapshot(snapshot)
	version, err := hashCatalogSnapshot(canonical)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	s.versions[version] = canonical
	s.mu.Unlock()
	return version, nil
}

func (s *catalogStore) get(version CatalogVersion) (CatalogSnapshot, bool) {
	s.mu.RLock()
	snapshot, ok := s.versions[version]
	s.mu.RUnlock()
	if !ok {
		return CatalogSnapshot{}, false
	}
	return canonicalizeCatalogSnapshot(snapshot), true
}

func canonicalizeCatalogSnapshot(snapshot CatalogSnapshot) CatalogSnapshot {
	return CatalogSnapshot{
		Schemas:    core.CanonicalizeSchemas(snapshot.Schemas),
		SearchPath: append([]string(nil), snapshot.SearchPath...),
	}
}

func hashCatalogSnapshot(snapshot CatalogSnapshot) (CatalogVersion, error) {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(raw)
	return CatalogVersion("sha256:" + hex.EncodeToString(sum[:])), nil
}
