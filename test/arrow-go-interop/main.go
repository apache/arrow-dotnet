// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command arrow-go-interop is built with -buildmode=c-shared and loaded by the
// .NET tests to exercise the Arrow C Async Device Stream Interface against
// arrow-go's independent implementation.
//
// Data enters and leaves this library through the synchronous C Stream
// Interface (ArrowArrayStream), which both libraries already support, so the
// only async hop in each test is the one under test:
//
//	.NET sync stream -> [Go async producer] -> .NET async consumer
//	.NET async producer -> [Go async consumer] -> .NET sync stream
//
// All exported functions return NULL on success or an error message that the
// caller must free with ArrowGoAsync_FreeError.
package main

/*
#include <stdint.h>
#include <stdlib.h>
#include "proxy.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/cgo"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

func main() {}

// InjectedErrorMessage is the message of the error injected by
// ArrowGoAsync_ProduceFromStream when failAfter is non-negative.
const injectedErrorMessage = "arrow-go-interop: injected producer failure"

func toCError(err error) *C.char {
	if err == nil {
		return nil
	}
	return C.CString(err.Error())
}

func recoverToError(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("arrow-go-interop: panic: %v", r)
	}
}

//export ArrowGoAsync_FreeError
func ArrowGoAsync_FreeError(msg *C.char) {
	C.free(unsafe.Pointer(msg))
}

// ArrowGoAsync_SetCrashOutput makes the Go runtime also write fatal errors
// (unrecovered panics in callbacks, etc.) to the given file. The test host
// usually swallows stderr, so otherwise a Go crash leaves no trace.
//
//export ArrowGoAsync_SetCrashOutput
func ArrowGoAsync_SetCrashOutput(path *C.char) (cErr *C.char) {
	f, err := os.OpenFile(C.GoString(path), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return toCError(err)
	}
	defer f.Close()
	return toCError(debug.SetCrashOutput(f, debug.CrashOptions{}))
}

// ArrowGoAsync_ProduceFromStream reads every batch from the given
// ArrowArrayStream and pushes it through the given ArrowAsyncDeviceStreamHandler
// using arrow-go's async producer. It blocks until the stream has been fully
// produced and the handler has been released.
//
// If failAfter is non-negative, the producer reports an error to the handler
// after delivering that many batches instead of continuing.
//
//export ArrowGoAsync_ProduceFromStream
func ArrowGoAsync_ProduceFromStream(source unsafe.Pointer, handler unsafe.Pointer, failAfter C.int64_t) (cErr *C.char) {
	var err error
	defer func() { cErr = toCError(err) }()
	defer recoverToError(&err)

	err = produceFromStream(
		(*cdata.CArrowArrayStream)(source),
		(*cdata.CArrowAsyncDeviceStreamHandler)(handler),
		int64(failAfter))
	return
}

func produceFromStream(source *cdata.CArrowArrayStream, handler *cdata.CArrowAsyncDeviceStreamHandler, failAfter int64) error {
	imported, err := cdata.ImportCRecordReader(source, nil)
	if err != nil {
		return fmt.Errorf("importing source stream: %w", err)
	}
	reader, ok := imported.(array.RecordReader)
	if !ok {
		return errors.New("imported source stream is not an array.RecordReader")
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema == nil {
		if reader.Err() != nil {
			return fmt.Errorf("reading source schema: %w", reader.Err())
		}
		return errors.New("source stream has no schema")
	}

	messages := make(chan arrowMessage)
	stop := make(chan struct{})
	go func() {
		defer close(messages)
		var sent int64
		for reader.Next() {
			if failAfter >= 0 && sent == failAfter {
				select {
				case messages <- arrowMessage{Err: errors.New(injectedErrorMessage)}:
				case <-stop:
				}
				return
			}
			rec := reader.RecordBatch()
			// The reader releases the current batch on the next call to Next;
			// the async producer releases it after extract_data.
			rec.Retain()
			select {
			case messages <- arrowMessage{Record: rec}:
				sent++
			case <-stop:
				rec.Release()
				return
			}
		}
		if reader.Err() != nil {
			select {
			case messages <- arrowMessage{Err: reader.Err()}:
			case <-stop:
			}
		}
	}()

	err = cdata.ExportAsyncRecordBatchStream(schema, messages, handler)

	// Unblock and drain the pump goroutine in case the producer stopped early.
	close(stop)
	for msg := range messages {
		if msg.Record != nil {
			msg.Record.Release()
		}
	}
	return err
}

type arrowMessage = cdata.RecordMessage

// consumer holds the state of one arrow-go async consumer created by
// ArrowGoAsync_ConsumerCreate.
type consumer struct {
	ctx    context.Context
	cancel context.CancelFunc
	ch     <-chan cdata.AsyncRecordBatchStream

	// Sits between the .NET producer and arrow-go; see proxy.h.
	proxy *C.struct_InteropProxy

	// Closed once arrow-go can no longer touch the proxy: its task goroutine
	// has exited (its record channel closed) or was never started.
	goDone     chan struct{}
	goDoneOnce sync.Once
}

func (c *consumer) markGoDone() {
	c.goDoneOnce.Do(func() { close(c.goDone) })
}

// ArrowGoAsync_ConsumerCreate populates the given ArrowAsyncDeviceStreamHandler
// so that it feeds arrow-go's async consumer. queueSize is the number of batches
// arrow-go requests up front; it then requests one more for each batch it
// consumes. Returns an opaque consumer id in *outId, which must eventually be
// passed to ArrowGoAsync_ConsumerFree.
//
//export ArrowGoAsync_ConsumerCreate
func ArrowGoAsync_ConsumerCreate(handler unsafe.Pointer, queueSize C.uint64_t, outId *C.uintptr_t) (cErr *C.char) {
	var err error
	defer func() { cErr = toCError(err) }()
	defer recoverToError(&err)

	if queueSize == 0 {
		err = errors.New("queueSize must be positive")
		return
	}

	proxy := C.InteropProxy_Create((*C.struct_ArrowAsyncDeviceStreamHandler)(handler))
	if proxy == nil {
		err = errors.New("could not allocate proxy")
		return
	}
	inner := unsafe.Pointer(C.InteropProxy_Inner(proxy))

	ctx, cancel := context.WithCancel(context.Background())
	ch := cdata.CreateAsyncDeviceStreamHandler(ctx, uint64(queueSize), (*cdata.CArrowAsyncDeviceStreamHandler)(inner))
	c := &consumer{ctx: ctx, cancel: cancel, ch: ch, proxy: proxy, goDone: make(chan struct{})}
	*outId = C.uintptr_t(cgo.NewHandle(c))
	return
}

// ArrowGoAsync_ConsumerToStream blocks until the producer has delivered the
// schema (or failed before doing so), then exports the consumed batches as a
// synchronous ArrowArrayStream into out. Errors the producer reports after the
// schema surface as errors from the exported stream's get_next.
//
//export ArrowGoAsync_ConsumerToStream
func ArrowGoAsync_ConsumerToStream(id C.uintptr_t, out unsafe.Pointer) (cErr *C.char) {
	var err error
	defer func() { cErr = toCError(err) }()
	defer recoverToError(&err)

	c := cgo.Handle(id).Value().(*consumer)
	stream, ok := <-c.ch
	if !ok {
		c.markGoDone()
		err = errors.New("async consumer closed without delivering a stream")
		return
	}
	if stream.Err != nil {
		// arrow-go only starts its task goroutine after a successful on_schema.
		c.markGoDone()
		err = stream.Err
		return
	}

	rdr := &channelReader{schema: stream.Schema, stream: stream.Stream, cancel: c.cancel, onClosed: c.markGoDone}
	rdr.refCount.Store(1)
	cdata.ExportRecordReader(rdr, (*cdata.CArrowArrayStream)(out))
	return
}

// ArrowGoAsync_ConsumerCancel asks the producer to stop by cancelling the
// consumer's context; arrow-go then calls the producer's cancel callback.
//
//export ArrowGoAsync_ConsumerCancel
func ArrowGoAsync_ConsumerCancel(id C.uintptr_t) {
	cgo.Handle(id).Value().(*consumer).cancel()
}

// ArrowGoAsync_ConsumerFree releases the consumer id. It does not release the
// handler; that is the producer's job. The proxy is freed in the background
// once the producer has released the handler and arrow-go is done with it; if
// either never happens (a bug the tests would report), it is leaked rather
// than freed while still in use.
//
//export ArrowGoAsync_ConsumerFree
func ArrowGoAsync_ConsumerFree(id C.uintptr_t) {
	h := cgo.Handle(id)
	c := h.Value().(*consumer)
	h.Delete()
	c.cancel()

	go func() {
		<-c.goDone
		for C.InteropProxy_IsReleased(c.proxy) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		C.InteropProxy_Free(c.proxy)
	}()
}

// channelReader adapts arrow-go's consumed batch channel to array.RecordReader
// so it can be exported as a synchronous ArrowArrayStream.
type channelReader struct {
	refCount atomic.Int64
	schema   *arrow.Schema
	stream   <-chan cdata.RecordMessage
	cancel   context.CancelFunc
	onClosed func() // called once the stream channel is seen closed
	cur      arrow.RecordBatch
	err      error
	once     sync.Once
}

func (r *channelReader) Retain() { r.refCount.Add(1) }

func (r *channelReader) Release() {
	if r.refCount.Add(-1) != 0 {
		return
	}
	r.once.Do(func() {
		r.releaseCurrent()
		// Stop the producer if the stream wasn't read to the end, and release
		// anything still queued so arrow-go's goroutines can exit.
		r.cancel()
		go func() {
			for msg := range r.stream {
				if msg.Record != nil {
					msg.Record.Release()
				}
			}
			r.onClosed()
		}()
	})
}

func (r *channelReader) releaseCurrent() {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}

func (r *channelReader) Schema() *arrow.Schema { return r.schema }

func (r *channelReader) Next() bool {
	r.releaseCurrent()
	if r.err != nil {
		return false
	}
	msg, ok := <-r.stream
	if !ok {
		r.onClosed()
		return false
	}
	if msg.Err != nil {
		r.err = msg.Err
		return false
	}
	r.cur = msg.Record
	return true
}

func (r *channelReader) RecordBatch() arrow.RecordBatch { return r.cur }

func (r *channelReader) Record() arrow.RecordBatch { return r.cur }

func (r *channelReader) Err() error { return r.err }
