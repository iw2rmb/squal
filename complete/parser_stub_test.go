package complete

import (
	"errors"

	"github.com/iw2rmb/squall/parser"
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
