package complete

const defaultMaxCandidates = 20

func normalizeRequest(req Request) Request {
	normalized := req
	if normalized.MaxCandidates <= 0 {
		normalized.MaxCandidates = defaultMaxCandidates
	}

	// Snippets are always enabled for completion responses.
	normalized.IncludeSnippets = true
	return normalized
}

func validateRequest(req Request) []Diagnostic {
	if req.SQL == "" {
		return []Diagnostic{
			{
				Code:    CatalogMissing,
				Message: "sql snapshot is required",
			},
		}
	}

	if req.CursorByte < 0 || req.CursorByte > len(req.SQL) {
		return []Diagnostic{
			{
				Code:    InvalidCursorSpan,
				Message: "cursor byte offset is out of bounds for sql snapshot",
			},
		}
	}

	return nil
}
