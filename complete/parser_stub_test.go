package complete

import (
	"errors"

	"github.com/iw2rmb/sql/parser"
)

type parserStub struct {
	metadata   *parser.QueryMetadata
	extractErr error
}

func (s *parserStub) ExtractMetadata(sql string) (*parser.QueryMetadata, error) {
	if s.extractErr != nil {
		return nil, s.extractErr
	}
	return s.metadata, nil
}

func (s *parserStub) NormalizeQuery(sql string) (string, error) {
	return sql, nil
}

func (s *parserStub) GenerateFingerprint(sql string) (string, error) {
	return "", nil
}

func (s *parserStub) ExtractTables(sql string) ([]string, error) {
	return nil, nil
}

func (s *parserStub) ExtractCaseAggregates(sql string) ([]parser.AggCase, error) {
	return nil, nil
}

func (s *parserStub) ExtractAggregateCompositions(sql string) ([]parser.AggComposition, error) {
	return nil, nil
}

func (s *parserStub) ExtractAggregates(sql string) ([]parser.Aggregate, error) {
	return nil, nil
}

func (s *parserStub) ExtractDistinctSpec(sql string) (*parser.DistinctSpec, error) {
	return nil, nil
}

func (s *parserStub) ExtractGroupBy(sql string) ([]parser.GroupItem, error) {
	return nil, nil
}

func (s *parserStub) ExtractTemporalOps(sql string) (*parser.TemporalOps, error) {
	return nil, nil
}

func (s *parserStub) ExtractJSONPaths(sql string) ([]parser.JSONPath, error) {
	return nil, nil
}

func (s *parserStub) DetectSlidingWindow(sql string) (*parser.SlidingWindowInfo, error) {
	return nil, nil
}

func healthyParserStub() *parserStub {
	return &parserStub{
		metadata: &parser.QueryMetadata{},
	}
}

func failedParserStub() *parserStub {
	return &parserStub{
		extractErr: errors.New("parser extract metadata failure"),
	}
}

func nilMetadataParserStub() *parserStub {
	return &parserStub{
		metadata: nil,
	}
}
