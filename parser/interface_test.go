package parser

import "testing"

type stubParser struct{}

func (s *stubParser) ExtractMetadata(sql string) (*QueryMetadata, error) {
	return nil, nil
}

func (s *stubParser) NormalizeQuery(sql string) (string, error) {
	return sql, nil
}

func (s *stubParser) GenerateFingerprint(sql string) (string, error) {
	return "", nil
}

func (s *stubParser) ExtractTables(sql string) ([]string, error) {
	return nil, nil
}

func (s *stubParser) ExtractCaseAggregates(sql string) ([]AggCase, error) {
	return nil, nil
}

func (s *stubParser) ExtractAggregateCompositions(sql string) ([]AggComposition, error) {
	return nil, nil
}

func (s *stubParser) ExtractAggregates(sql string) ([]Aggregate, error) {
	return nil, nil
}

func (s *stubParser) ExtractDistinctSpec(sql string) (*DistinctSpec, error) {
	return nil, nil
}

func (s *stubParser) ExtractGroupBy(sql string) ([]GroupItem, error) {
	return nil, nil
}

func (s *stubParser) ExtractTemporalOps(sql string) (*TemporalOps, error) {
	return nil, nil
}

func (s *stubParser) ExtractJSONPaths(sql string) ([]JSONPath, error) {
	return nil, nil
}

func (s *stubParser) DetectSlidingWindow(sql string) (*SlidingWindowInfo, error) {
	return nil, nil
}

func TestNewTestParserPanicsWithoutFactory(t *testing.T) {
	prev := testParserFactory
	t.Cleanup(func() {
		testParserFactory = prev
	})
	testParserFactory = nil

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic but got none")
		}
		got, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected panic string, got %T", recovered)
		}
		const want = "parser: PG test parser factory not registered; build with CGO enabled"
		if got != want {
			t.Fatalf("panic message mismatch: got %q, want %q", got, want)
		}
	}()

	_ = NewTestParser()
}

func TestRegisterTestParserFactoryAndNewTestParser(t *testing.T) {
	prev := testParserFactory
	t.Cleanup(func() {
		testParserFactory = prev
	})

	expected := &stubParser{}
	RegisterTestParserFactory(func() Parser {
		return expected
	})

	got := NewTestParser()
	if got != expected {
		t.Fatalf("NewTestParser() returned %T, want %T", got, expected)
	}
}
