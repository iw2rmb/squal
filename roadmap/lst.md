# LST Platform Implementation

Scope: Implement SQL structural tree (`lst`) in this repository with deterministic query/traverse/mutate APIs and lossless emit guarantees for untouched byte ranges.

Documentation:
- `research/sql.md`
- `design/sql.md`
- `design/parser.md`
- `design/lst.md`
- `roadmap/parser.md`
- `roadmap/sql/phase-1-parser-contracts.md`

Legend: [ ] todo, [x] done.

## Implementation Steps
- [ ] Add shared text contracts required by `lst` — Provide canonical span/edit DTO ownership before introducing `lst` APIs that depend on them.
  - Repository: `sql`
  - Component: `core`
  - Scope: Extend `core` with `Span`, `TextEdit`, and `TextChangeSet` contracts with byte-oriented indexing and deterministic validation helpers.
  - Snippets: `type Span struct { StartByte int; EndByte int }`
  - Tests: `go test ./core/...` — shared text DTO serialization and validation logic compile and pass.

- [ ] Create `lst` package/module skeleton — Establish package boundary and dependency wiring for `lst`.
  - Repository: `sql`
  - Component: `lst`
  - Scope: Add `lst` package files and (if required by module strategy) `lst/go.mod`; wire imports to `core` and `parser` only.
  - Snippets: `package lst`
  - Tests: `go test ./lst/...` — package builds and baseline tests run.

- [ ] Implement LST node/document model — Define stable structural representation consumed by traversal, mutation, and emit.
  - Repository: `sql`
  - Component: `lst`
  - Scope: Add `Document`, `Node`, `NodeID`, node kind registry, parent/child invariants, and span containment rules.
  - Snippets: `type Node struct { ID NodeID; Kind NodeKind; Span core.Span }`
  - Tests: `go test ./lst/... -run 'Test(Document|Node|NodeID)'` — model invariants and identity rules pass.

- [ ] Implement capture taxonomy and registry — Provide deterministic semantic indexing for query/traverse/mutate consumers.
  - Repository: `sql`
  - Component: `lst`
  - Scope: Add `CaptureKind` registry, `Capture` DTO, index construction, and lookup APIs by kind/node/span.
  - Snippets: `type CaptureKind string`
  - Tests: `go test ./lst/... -run 'TestCapture'` — ordering and lookup determinism are stable.

- [ ] Implement parse-to-LST builder pipeline — Convert parser outputs and SQL bytes into deterministic LST documents.
  - Repository: `sql`
  - Component: `lst`
  - Scope: Build LST from `parser` outputs, assign stable node IDs for unchanged structure, and preserve parser diagnostics snapshot in `Document`.
  - Snippets: `func Build(sql []byte, parsed parser.QueryMetadata) (Document, []Diagnostic, error)`
  - Tests: `go test ./lst/... -run 'TestBuild'` — equivalent inputs produce equivalent structure and IDs.

- [ ] Implement deterministic traversal and query APIs — Expose canonical operations for downstream `complete` and adapter callers.
  - Repository: `sql`
  - Component: `lst`
  - Scope: Add pre-order DFS traversal, node lookup by ID, capture queries, and span-overlap selectors with stable result ordering.
  - Snippets: `func (d Document) Walk(fn func(Node) bool)`
  - Tests: `go test ./lst/... -run 'TestWalk|TestQuery'` — traversal and query ordering remain deterministic.

- [ ] Implement mutation planning and validation — Provide safe edit plans with deterministic conflict handling.
  - Repository: `sql`
  - Component: `lst`
  - Scope: Add mutation API producing validated `core.TextChangeSet`; reject overlapping edits, invalid spans, and unsupported node operations with typed diagnostics.
  - Snippets: `func (d Document) PlanMutations(req []MutationRequest) (core.TextChangeSet, []Diagnostic, error)`
  - Tests: `go test ./lst/... -run 'TestMutate|TestOverlappingEdits|TestInvalidSpan'` — validation and diagnostics behavior is stable.

- [ ] Implement emitter with lossless untouched-span guarantees — Ensure byte-preserving roundtrip behavior for no-op and targeted edit flows.
  - Repository: `sql`
  - Component: `lst`
  - Scope: Add emit engine that copies untouched byte ranges verbatim and rewrites only changed spans from planned changes.
  - Snippets: `func EmitWithChanges(doc Document, cs core.TextChangeSet) ([]byte, []Diagnostic, error)`
  - Tests: `go test ./lst/... -run 'TestEmit|TestRoundtrip'` — no-op output matches input byte-for-byte and targeted rewrites are bounded.

- [ ] Add golden, fuzz, and Unicode correctness coverage — Lock deterministic behavior and byte/rune correctness under varied inputs.
  - Repository: `sql`
  - Component: `lst` tests
  - Scope: Add fixtures for parse/build/emit scenarios, fuzz tests for mutation span handling, and Unicode-heavy cases with mixed rune widths.
  - Snippets: `go test ./lst/... -run TestGolden`
  - Tests: `go test ./lst/...` and `go test ./lst/... -run Fuzz -fuzz=Fuzz -fuzztime=10s` — deterministic and Unicode-safe behavior holds.

- [ ] Add integration contract fixtures for `complete` consumers — Freeze handoff semantics so `complete` can be implemented without ambiguity.
  - Repository: `sql`
  - Component: `lst`, shared fixtures
  - Scope: Add stable fixture format and helper APIs for completion-oriented context extraction (scope node, visible aliases, join edges, projection targets).
  - Snippets: `func ContextAt(doc Document, pos int) (Context, []Diagnostic, error)`
  - Tests: `go test ./lst/... -run 'TestContextAt'` — context extraction is stable and deterministic.

- [ ] Run acceptance gate and document completion status — Mark phase complete only after deterministic and dependency guarantees are proven.
  - Repository: `sql`
  - Component: `core`, `lst`, docs
  - Scope: Execute full test suite for affected packages, verify dependency boundaries, and update roadmap checkbox states to reflect reality.
  - Snippets: `rg -n "parserpg|aster/internal" lst`
  - Tests: `go test ./core/... ./parser/... ./lst/...` and `rg -n "parserpg|aster/internal" lst` — all checks pass and forbidden imports are absent.
