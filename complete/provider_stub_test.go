package complete

import "errors"

var errProviderUnavailable = errors.New("provider unavailable")

type providerStub struct {
	result ProviderResult
	err    error
}

func (s *providerStub) Complete(req Request) (ProviderResult, error) {
	if s.err != nil {
		return ProviderResult{}, s.err
	}
	return s.result, nil
}
