package cdc

import (
	"context"
	"errors"
	"testing"
)

type mockCheckpointStore struct {
	savedLSN  LSN
	ackedLSN  LSN
	saveCalls int
	ackCalls  int
	saveErr   error
	ackErr    error
	loadedLSN LSN
	loadErr   error
}

func (m *mockCheckpointStore) LoadCheckpoint(context.Context, SlotName) (LSN, error) {
	if m.loadErr != nil {
		return "", m.loadErr
	}
	return m.loadedLSN, nil
}

func (m *mockCheckpointStore) SaveCheckpoint(_ context.Context, _ SlotName, lsn LSN) error {
	m.saveCalls++
	if m.saveErr != nil {
		return m.saveErr
	}
	m.savedLSN = lsn
	return nil
}

func (m *mockCheckpointStore) AckLSN(_ context.Context, lsn LSN) error {
	m.ackCalls++
	m.ackedLSN = lsn
	if m.ackErr != nil {
		return m.ackErr
	}
	return nil
}

type mockBatchHandler struct {
	calls int
	err   error
}

func (m *mockBatchHandler) HandleBatch(context.Context, TxBatch) error {
	m.calls++
	return m.err
}

func TestConsumer_CallbackDispatcherCheckpointSequence(t *testing.T) {
	handler := &mockBatchHandler{}
	store := &mockCheckpointStore{}
	dispatcher := callbackDispatcher{logger: &probeLogger{}}

	batch := TxBatch{LSN: "0/16", Events: []TxEvent{{Table: "public.t", Operation: OpInsert}}}
	err := dispatcher.DispatchWithCheckpoint(context.Background(), batch, SlotName("slot"), store, handler)
	if err != nil {
		t.Fatalf("DispatchWithCheckpoint() error = %v", err)
	}
	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", handler.calls)
	}
	if store.saveCalls != 1 {
		t.Fatalf("SaveCheckpoint calls = %d, want 1", store.saveCalls)
	}
	if store.ackCalls != 1 {
		t.Fatalf("AckLSN calls = %d, want 1", store.ackCalls)
	}
}

func TestConsumer_CallbackDispatcherErrors(t *testing.T) {
	tests := []struct {
		name          string
		handlerErr    error
		saveErr       error
		ackErr        error
		wantErr       bool
		wantSaveCalls int
		wantAckCalls  int
	}{
		{
			name:          "handler error stops flow",
			handlerErr:    errors.New("handler failed"),
			wantErr:       true,
			wantSaveCalls: 0,
			wantAckCalls:  0,
		},
		{
			name:          "save error returns error",
			saveErr:       errors.New("save failed"),
			wantErr:       true,
			wantSaveCalls: 1,
			wantAckCalls:  0,
		},
		{
			name:          "ack error is best effort",
			ackErr:        errors.New("ack failed"),
			wantErr:       false,
			wantSaveCalls: 1,
			wantAckCalls:  1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			handler := &mockBatchHandler{err: tt.handlerErr}
			store := &mockCheckpointStore{saveErr: tt.saveErr, ackErr: tt.ackErr}
			dispatcher := callbackDispatcher{logger: &probeLogger{}}

			err := dispatcher.DispatchWithCheckpoint(context.Background(), TxBatch{LSN: "0/16"}, SlotName("slot"), store, handler)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DispatchWithCheckpoint() error = %v, wantErr=%v", err, tt.wantErr)
			}
			if store.saveCalls != tt.wantSaveCalls {
				t.Fatalf("SaveCheckpoint calls = %d, want %d", store.saveCalls, tt.wantSaveCalls)
			}
			if store.ackCalls != tt.wantAckCalls {
				t.Fatalf("AckLSN calls = %d, want %d", store.ackCalls, tt.wantAckCalls)
			}
		})
	}
}
