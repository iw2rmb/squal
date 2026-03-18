package decomposition

import (
	"encoding/json"
	"testing"
)

// TestDecompositionConfigJSONRoundTrip verifies that DecompositionConfig can be marshaled
// and unmarshaled to/from JSON while preserving numeric values in the correct format.
func TestDecompositionConfigJSONRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		config *DecompositionConfig
	}{
		{
			name: "default config",
			config: &DecompositionConfig{
				MaxSubqueryDepth:   5,
				MinCachingScore:    0.5,
				MaxComplexityScore: 100,
				AnalysisTimeoutMS:  1000,
				EnableCTEAnalysis:  true,
				EnableCorrelatedSQ: false,
				SubqueryCacheTTL:   3600,
				MaxSubqueryCache:   1000,
			},
		},
		{
			name: "custom values",
			config: &DecompositionConfig{
				MaxSubqueryDepth:   10,
				MinCachingScore:    0.75,
				MaxComplexityScore: 200,
				AnalysisTimeoutMS:  5000,
				EnableCTEAnalysis:  false,
				EnableCorrelatedSQ: true,
				SubqueryCacheTTL:   7200,
				MaxSubqueryCache:   2000,
			},
		},
		{
			name: "zero values",
			config: &DecompositionConfig{
				MaxSubqueryDepth:   0,
				MinCachingScore:    0.0,
				MaxComplexityScore: 0,
				AnalysisTimeoutMS:  0,
				EnableCTEAnalysis:  false,
				EnableCorrelatedSQ: false,
				SubqueryCacheTTL:   0,
				MaxSubqueryCache:   0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			data, err := json.Marshal(tt.config)
			if err != nil {
				t.Fatalf("failed to marshal config: %v", err)
			}

			// Unmarshal back to struct
			var unmarshaled DecompositionConfig
			if err := json.Unmarshal(data, &unmarshaled); err != nil {
				t.Fatalf("failed to unmarshal config: %v", err)
			}

			// Verify all fields match
			if unmarshaled.MaxSubqueryDepth != tt.config.MaxSubqueryDepth {
				t.Errorf("MaxSubqueryDepth mismatch: got %d, want %d", unmarshaled.MaxSubqueryDepth, tt.config.MaxSubqueryDepth)
			}
			if unmarshaled.MinCachingScore != tt.config.MinCachingScore {
				t.Errorf("MinCachingScore mismatch: got %f, want %f", unmarshaled.MinCachingScore, tt.config.MinCachingScore)
			}
			if unmarshaled.MaxComplexityScore != tt.config.MaxComplexityScore {
				t.Errorf("MaxComplexityScore mismatch: got %d, want %d", unmarshaled.MaxComplexityScore, tt.config.MaxComplexityScore)
			}
			if unmarshaled.AnalysisTimeoutMS != tt.config.AnalysisTimeoutMS {
				t.Errorf("AnalysisTimeoutMS mismatch: got %d, want %d", unmarshaled.AnalysisTimeoutMS, tt.config.AnalysisTimeoutMS)
			}
			if unmarshaled.EnableCTEAnalysis != tt.config.EnableCTEAnalysis {
				t.Errorf("EnableCTEAnalysis mismatch: got %t, want %t", unmarshaled.EnableCTEAnalysis, tt.config.EnableCTEAnalysis)
			}
			if unmarshaled.EnableCorrelatedSQ != tt.config.EnableCorrelatedSQ {
				t.Errorf("EnableCorrelatedSQ mismatch: got %t, want %t", unmarshaled.EnableCorrelatedSQ, tt.config.EnableCorrelatedSQ)
			}
			if unmarshaled.SubqueryCacheTTL != tt.config.SubqueryCacheTTL {
				t.Errorf("SubqueryCacheTTL mismatch: got %d, want %d", unmarshaled.SubqueryCacheTTL, tt.config.SubqueryCacheTTL)
			}
			if unmarshaled.MaxSubqueryCache != tt.config.MaxSubqueryCache {
				t.Errorf("MaxSubqueryCache mismatch: got %d, want %d", unmarshaled.MaxSubqueryCache, tt.config.MaxSubqueryCache)
			}
		})
	}
}

// TestDecompositionConfigJSONFieldNames verifies that JSON field names are preserved
// and that the time fields marshal as integers.
func TestDecompositionConfigJSONFieldNames(t *testing.T) {
	config := &DecompositionConfig{
		MaxSubqueryDepth:   5,
		MinCachingScore:    0.5,
		MaxComplexityScore: 100,
		AnalysisTimeoutMS:  1000,
		EnableCTEAnalysis:  true,
		EnableCorrelatedSQ: false,
		SubqueryCacheTTL:   3600,
		MaxSubqueryCache:   1000,
	}

	// Marshal to JSON
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	// Parse as map to verify field names and types
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to unmarshal to map: %v", err)
	}

	// Verify field names
	expectedFields := []string{
		"max_subquery_depth",
		"min_caching_score",
		"max_complexity_score",
		"analysis_timeout_ms",
		"enable_cte_analysis",
		"enable_correlated_subqueries",
		"subquery_cache_ttl_seconds",
		"max_subquery_cache_entries",
	}

	for _, field := range expectedFields {
		if _, ok := m[field]; !ok {
			t.Errorf("missing expected JSON field: %s", field)
		}
	}

	// Verify time fields are numeric (not objects)
	if timeoutVal, ok := m["analysis_timeout_ms"].(float64); !ok {
		t.Errorf("analysis_timeout_ms should be numeric, got type %T", m["analysis_timeout_ms"])
	} else if int64(timeoutVal) != 1000 {
		t.Errorf("analysis_timeout_ms should be 1000, got %v", timeoutVal)
	}

	if ttlVal, ok := m["subquery_cache_ttl_seconds"].(float64); !ok {
		t.Errorf("subquery_cache_ttl_seconds should be numeric, got type %T", m["subquery_cache_ttl_seconds"])
	} else if int64(ttlVal) != 3600 {
		t.Errorf("subquery_cache_ttl_seconds should be 3600, got %v", ttlVal)
	}
}

// TestDecompositionConfigDurationConversion verifies that the time wrappers
// correctly convert to time.Duration.
func TestDecompositionConfigDurationConversion(t *testing.T) {
	config := DefaultDecompositionConfig()

	// Test AnalysisTimeoutMS conversion
	timeoutDuration := config.AnalysisTimeoutMS.Duration()
	expectedTimeout := Milliseconds(1000).Duration()
	if timeoutDuration != expectedTimeout {
		t.Errorf("AnalysisTimeoutMS.Duration() = %v, want %v", timeoutDuration, expectedTimeout)
	}

	// Test SubqueryCacheTTL conversion
	ttlDuration := config.SubqueryCacheTTL.Duration()
	expectedTTL := Seconds(3600).Duration()
	if ttlDuration != expectedTTL {
		t.Errorf("SubqueryCacheTTL.Duration() = %v, want %v", ttlDuration, expectedTTL)
	}
}
