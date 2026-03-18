package cdc

import (
	"context"
	"reflect"
	"testing"
	"time"
)

var _ BatchHandler = BatchHandlerFunc(func(context.Context, TxBatch) error { return nil })

func TestTxEventContract(t *testing.T) {
	t.Parallel()

	assertStructFields(t, reflect.TypeOf(TxEvent{}), []fieldSpec{
		{Name: "CommitLSN", Type: reflect.TypeOf(LSN("")), JSONTag: "commit_lsn"},
		{Name: "Table", Type: reflect.TypeOf(""), JSONTag: "table"},
		{Name: "Operation", Type: reflect.TypeOf(Op("")), JSONTag: "operation"},
		{Name: "CommitTime", Type: reflect.TypeOf(time.Time{}), JSONTag: "commit_time"},
		{Name: "Keys", Type: reflect.TypeOf(map[string]any(nil)), JSONTag: "keys,omitempty"},
		{Name: "Old", Type: reflect.TypeOf(map[string]any(nil)), JSONTag: "old,omitempty"},
		{Name: "New", Type: reflect.TypeOf(map[string]any(nil)), JSONTag: "new,omitempty"},
	})
}

func TestTxBatchContract(t *testing.T) {
	t.Parallel()

	assertStructFields(t, reflect.TypeOf(TxBatch{}), []fieldSpec{
		{Name: "LSN", Type: reflect.TypeOf(LSN("")), JSONTag: "lsn"},
		{Name: "Events", Type: reflect.TypeOf([]TxEvent(nil)), JSONTag: "events"},
	})
}

func TestCheckpointContracts(t *testing.T) {
	t.Parallel()

	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	slotType := reflect.TypeOf(SlotName(""))
	lsnType := reflect.TypeOf(LSN(""))
	errorType := reflect.TypeOf((*error)(nil)).Elem()

	checkpointSaver := reflect.TypeOf((*CheckpointSaver)(nil)).Elem()
	assertInterfaceMethod(t, checkpointSaver, "SaveCheckpoint", []reflect.Type{contextType, slotType, lsnType}, []reflect.Type{errorType})
	assertInterfaceMethod(t, checkpointSaver, "AckLSN", []reflect.Type{contextType, lsnType}, []reflect.Type{errorType})

	checkpointLoader := reflect.TypeOf((*CheckpointLoader)(nil)).Elem()
	assertInterfaceMethod(t, checkpointLoader, "LoadCheckpoint", []reflect.Type{contextType, slotType}, []reflect.Type{lsnType, errorType})

	checkpointStore := reflect.TypeOf((*CheckpointStore)(nil)).Elem()
	assertInterfaceMethod(t, checkpointStore, "SaveCheckpoint", []reflect.Type{contextType, slotType, lsnType}, []reflect.Type{errorType})
	assertInterfaceMethod(t, checkpointStore, "AckLSN", []reflect.Type{contextType, lsnType}, []reflect.Type{errorType})
	assertInterfaceMethod(t, checkpointStore, "LoadCheckpoint", []reflect.Type{contextType, slotType}, []reflect.Type{lsnType, errorType})
}

func TestEventHandlerContract(t *testing.T) {
	t.Parallel()

	eventHandler := reflect.TypeOf((EventHandler)(nil))
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	txBatchType := reflect.TypeOf(TxBatch{})
	errorType := reflect.TypeOf((*error)(nil)).Elem()

	if eventHandler.Kind() != reflect.Func {
		t.Fatalf("EventHandler kind = %s, want func", eventHandler.Kind())
	}
	if eventHandler.NumIn() != 2 {
		t.Fatalf("EventHandler inputs = %d, want 2", eventHandler.NumIn())
	}
	if eventHandler.In(0) != contextType {
		t.Fatalf("EventHandler arg0 = %v, want %v", eventHandler.In(0), contextType)
	}
	if eventHandler.In(1) != txBatchType {
		t.Fatalf("EventHandler arg1 = %v, want %v", eventHandler.In(1), txBatchType)
	}
	if eventHandler.NumOut() != 1 {
		t.Fatalf("EventHandler outputs = %d, want 1", eventHandler.NumOut())
	}
	if eventHandler.Out(0) != errorType {
		t.Fatalf("EventHandler return = %v, want %v", eventHandler.Out(0), errorType)
	}
}

func TestBatchHandlerFuncAdapter(t *testing.T) {
	t.Parallel()

	want := TxBatch{LSN: "0/16"}
	called := false

	handler := BatchHandlerFunc(func(_ context.Context, batch TxBatch) error {
		called = true
		if !reflect.DeepEqual(batch, want) {
			t.Fatalf("batch = %+v, want %+v", batch, want)
		}
		return nil
	})

	if err := handler.HandleBatch(context.Background(), want); err != nil {
		t.Fatalf("HandleBatch returned error: %v", err)
	}
	if !called {
		t.Fatal("handler callback was not called")
	}
}

type fieldSpec struct {
	Name    string
	Type    reflect.Type
	JSONTag string
}

func assertStructFields(t *testing.T, typ reflect.Type, expected []fieldSpec) {
	t.Helper()

	for _, field := range expected {
		got, ok := typ.FieldByName(field.Name)
		if !ok {
			t.Fatalf("%s missing field %q", typ.Name(), field.Name)
		}
		if got.Type != field.Type {
			t.Fatalf("%s.%s type = %v, want %v", typ.Name(), field.Name, got.Type, field.Type)
		}
		if got.Tag.Get("json") != field.JSONTag {
			t.Fatalf("%s.%s json tag = %q, want %q", typ.Name(), field.Name, got.Tag.Get("json"), field.JSONTag)
		}
	}
}

func assertInterfaceMethod(t *testing.T, iface reflect.Type, methodName string, inputs []reflect.Type, outputs []reflect.Type) {
	t.Helper()

	method, ok := iface.MethodByName(methodName)
	if !ok {
		t.Fatalf("%s missing method %s", iface.Name(), methodName)
	}

	signature := method.Type
	if signature.NumIn() != len(inputs) {
		t.Fatalf("%s.%s inputs = %d, want %d", iface.Name(), methodName, signature.NumIn(), len(inputs))
	}
	for i, want := range inputs {
		if got := signature.In(i); got != want {
			t.Fatalf("%s.%s input %d = %v, want %v", iface.Name(), methodName, i, got, want)
		}
	}

	if signature.NumOut() != len(outputs) {
		t.Fatalf("%s.%s outputs = %d, want %d", iface.Name(), methodName, signature.NumOut(), len(outputs))
	}
	for i, want := range outputs {
		if got := signature.Out(i); got != want {
			t.Fatalf("%s.%s output %d = %v, want %v", iface.Name(), methodName, i, got, want)
		}
	}
}
