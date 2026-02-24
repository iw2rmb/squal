# SQL Completion Relevance Hardening

Scope: Eliminate irrelevant or semantically-wrong completion candidates at SQL entry points, while preserving deterministic ranking and parser fallback availability.

Documentation:
- `research/sql.md`
- `docs/parser-migration.md`
- `roadmap/comp.md`
- `parser/interface.go`
- `complete/context.go`
- `complete/context_builder.go`
- `complete/candidate_generation.go`
- `complete/ranking.go`

Legend: [ ] todo, [x] done.

## Historical Design Basis (Recovered From Git History)
- `design/completions.md` was deleted in commit `f482e2e` (2026-02-24). This roadmap now carries the design constraints required for ongoing completion hardening work.
- Deterministic behavior remains mandatory: identical request identity must produce stable candidate ordering and stable diagnostics.
- Parser-degraded fallback remains mandatory: completion must remain available with explicit `ParseDegraded` diagnostics.
- Relevance hardening must be clause-aware: expression clauses must avoid global schema/table noise when parser metadata is unavailable.
- Package boundaries remain unchanged: `complete` must stay parser-neutral and must not import `parserpg`.

## Phase 1 — Parse-Degraded Context Recovery
- [x] Preserve clause-awareness when parser metadata extraction fails — Prevent global/noisy fallback candidates at `WHERE`, `JOIN ... ON`, `GROUP BY`, and `ORDER BY` entry points.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Update `buildContext` in `complete/context_builder.go` so parser errors return `completionContext{ActiveClause: activeClauseAtCursor(req.SQL, req.CursorByte), ParseDegraded: true}` instead of only `ParseDegraded`. Add helper for this fallback shape to keep diagnostics behavior unchanged.
  - Snippets:
    ```go
    if err != nil {
        return completionContext{
            ActiveClause:  activeClauseAtCursor(req.SQL, req.CursorByte),
            ParseDegraded: true,
        }, []Diagnostic{{Code: ParseDegraded, Message: "parser metadata extraction failed"}}
    }
    ```
  - Tests: `go test ./complete/... -run 'TestParseDegraded'` — degraded responses still include `ParseDegraded`, and now carry clause-aware degraded context.

- [x] Add degraded-mode candidate policy by clause — Keep completion available without flooding with irrelevant schema/table objects.
  - Repository: `sql`
  - Component: `complete`
  - Scope: In `complete/candidate_generation.go`, gate candidate families when `ctx.ParseDegraded` is true:
    - `WHERE|GROUP BY|ORDER BY|JOIN ON`: prioritize columns/keywords/snippets, suppress global schema/table families.
    - `FROM`: keep table candidates.
    - `FROM tail`: keep continuation keywords/joins, suppress table/column.
  - Snippets:
    ```go
    if ctx.ParseDegraded && isExpressionClause(ctx.ActiveClause) {
        addKeywordCandidates(out, ctx)
        addSnippetCandidates(out)
        return out.finalize(req.MaxCandidates)
    }
    ```
  - Tests: add `TestParseDegradedClauseScopedCandidates` in `complete/candidate_generation_test.go` — verify no `CandidateKindTable`/`CandidateKindSchema` for degraded `... where ` and degraded `... join ... on `, while degraded `from` still returns table candidates.

## Phase 2 — Token-Aware Clause Detection
- [x] Replace raw substring clause matching with token-aware scanning — Avoid false context switches caused by keywords in string literals, comments, or identifiers.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Refactor `activeClauseAtCursor` in `complete/context.go`:
    - Scan SQL prefix byte-by-byte.
    - Ignore content inside `'...'`, `"..."`, `-- ...`, and `/* ... */`.
    - Match clause keywords on token boundaries only.
  - Snippets:
    ```go
    // pseudo
    for i < len(prefix) {
        switch state {
        case inSingleQuote, inDoubleQuote, inLineComment, inBlockComment:
            advanceState(...)
        default:
            if boundaryKeywordAt(prefix, i, "WHERE") { last = contextClauseWhere }
        }
    }
    ```
  - Tests: add `TestActiveClauseIgnoresQuotedKeywords` and `TestActiveClauseIgnoresCommentKeywords` in `complete/context_builder_test.go` — examples:
    - `SELECT 'from' AS s`
    - `SELECT 1 -- where`
    - `SELECT "join" FROM t`

- [x] Keep existing `FROM` vs `FROM tail` semantics after lexer refactor — Preserve newly-correct behavior around `FROM <table> _`.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Ensure `classifyFromClause` still handles:
    - `SELECT ... FROM _` → `from`
    - `SELECT ... FROM orders _` → `from_tail`
    - `SELECT ... FROM orders, _` → `from`
  - Snippets:
    ```go
    if fromClauseNeedsTable(tail) { return contextClauseFrom }
    return contextClauseFromTail
    ```
  - Tests: keep and extend existing `TestActiveClauseAtCursorFromTail` / `TestActiveClauseAtCursorFromAfterCommaNeedsTable`.

## Phase 3 — Dedicated JOIN-ON Entry Context
- [x] Introduce explicit `JOIN ON` context — Stop ranking table names above predicate-building objects at `... ON _`.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Add `contextClauseJoinOn` in `complete/context.go` and classify when cursor is after `ON` belonging to current join expression.
  - Snippets:
    ```go
    const contextClauseJoinOn contextClause = "join_on"
    // classify JOIN tail: ... JOIN <t> ON <cursor>
    ```
  - Tests: add `TestActiveClauseAtCursorJoinOn` in `complete/context_builder_test.go`.

- [x] Tune candidate families and ranking for `JOIN ON` — Prefer join-condition composition over unrelated object-name completions.
  - Repository: `sql`
  - Component: `complete`
  - Scope:
    - In `complete/candidate_generation.go`, for `join_on`: emit visible table columns and boolean/comparison keywords; suppress global schema/table candidates.
    - In `complete/ranking.go`, add `contextClauseJoinOn` weights: `column` highest, then `keyword`, then limited snippets.
  - Snippets:
    ```go
    // ranking target
    {contextClauseJoinOn, CandidateKindColumn}: 60
    {contextClauseJoinOn, CandidateKindKeyword}: 40
    ```
  - Tests: add `TestJoinOnPrefersColumnsAndPredicates` in `complete/candidate_generation_test.go` — for `... join customers c on ` ensure top candidates are `o.*`, `c.*`, and predicate keywords, not unrelated tables.

## Phase 4 — Context-Gated Global Candidate Families
- [x] Gate schema candidates to object-name entry points — Reduce noise in expression clauses.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Update `generateCandidates`/`addSchemaCandidates` usage so `CandidateKindSchema` is emitted only where schema qualification is expected (`SELECT` without bound source, `FROM`, `JOIN` table target).
  - Snippets:
    ```go
    if allowsSchemaCandidates(ctx.ActiveClause, ctx.ParseDegraded) {
        addSchemaCandidates(out, idx)
    }
    ```
  - Tests: add `TestWhereSuppressesSchemaCandidates` and `TestJoinOnSuppressesSchemaCandidates`.

- [x] Gate snippet families by clause — Keep snippets contextual and avoid suggesting structural templates in expression tails.
  - Repository: `sql`
  - Component: `complete`
  - Scope:
    - Remove request-level snippet toggling; completion always emits clause-appropriate snippets.
    - Change emission policy in `addSnippetCandidates` call-site:
      - allow `SELECT ... FROM ...` mainly in `unknown/select`.
      - allow `JOIN ... ON ...` and `WHERE ...` in `from_tail`.
      - suppress broad snippets in `where/group/order/join_on`.
  - Snippets:
    ```go
    if allowsSnippetsForClause(ctx.ActiveClause) {
        addSnippetCandidates(out)
    }
    ```
  - Tests: add `TestClauseScopedSnippets` to verify no `SELECT ... FROM ...` snippet at `... where ` and `... join ... on `.

## Phase 5 — Regression Coverage, Golden Fixtures, and Acceptance
- [x] Add golden coverage for wrong-entry regressions — Lock ranking and families for critical cursor positions.
  - Repository: `sql`
  - Component: `complete` tests
  - Scope: Extend `complete/golden_test.go` and add fixtures under `complete/testdata/candidates/` for:
    - `from_tail` continuation (`SELECT * FROM orders _`)
    - degraded `where` / degraded `join on`
    - join-on expression context
    - quoted/comment keyword safety cases
  - Snippets:
    ```bash
    go test ./complete/... -run TestGoldenCandidates -update
    ```
  - Tests: `go test ./complete/...` — deterministic fixture output and relevance assertions pass.

- [x] Run acceptance gate and synchronize design/roadmap docs — Keep repository documentation aligned with implemented behavior.
  - Repository: `sql`
  - Component: `complete`, docs
  - Scope: After implementation, update:
    - this file `roadmap/comp.md` (phase status + historical design constraints),
    - `docs/parser-migration.md` and `research/sql.md` references if contract/ownership statements change.
  - Snippets:
    ```bash
    go test ./core/... ./parser/... ./complete/...
    rg -n "contextClauseJoinOn|ParseDegraded|FROM-tail" complete roadmap docs research
    ```
  - Tests: Full target suite green; docs accurately describe actual completion behavior and fallback semantics.
