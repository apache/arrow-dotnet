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

// WORKAROUND for an arrow-go (v18.8.0) consumer bug: after the producer calls
// release on the handler, arrow-go's task goroutine keeps draining tasks that
// were already queued and calls request(1) on the producer for each one. The
// spec only requires the producer to stay valid until release, so a conforming
// producer (like Apache.Arrow's) may already have freed it: use-after-free.
//
// The proxy sits between the .NET producer and arrow-go's consumer:
//
//   .NET producer --> outer handler (proxy callbacks) --> inner handler (arrow-go)
//   arrow-go      --> proxy producer (owned here)     --> real .NET producer
//
// The proxy producer forwards request/cancel to the real producer only until the
// outer handler is released, and stays allocated until arrow-go is done with it.
// Remove this once arrow-go is fixed: https://github.com/apache/arrow-go/issues/1572
//
// The proxy also works around a second arrow-go (v18.8.0) bug: after a failed
// on_schema, arrow-go's own release callback uses state it has already freed, so
// the proxy doesn't forward release in that case. See outer_release and
// https://github.com/apache/arrow-go/issues/1573

#pragma once

#include <stdint.h>

#ifndef ARROW_C_ASYNC_STREAM_INTERFACE
#define ARROW_C_ASYNC_STREAM_INTERFACE

// Layouts from the Arrow C Device Data Interface specification.
typedef int32_t ArrowDeviceType;

struct ArrowSchema;
struct ArrowAsyncTask;

struct ArrowAsyncProducer {
  ArrowDeviceType device_type;
  void (*request)(struct ArrowAsyncProducer* self, int64_t n);
  void (*cancel)(struct ArrowAsyncProducer* self);
  const char* additional_metadata;
  void* private_data;
};

struct ArrowAsyncDeviceStreamHandler {
  int (*on_schema)(struct ArrowAsyncDeviceStreamHandler* self,
                   struct ArrowSchema* stream_schema);
  int (*on_next_task)(struct ArrowAsyncDeviceStreamHandler* self,
                      struct ArrowAsyncTask* task, const char* metadata);
  void (*on_error)(struct ArrowAsyncDeviceStreamHandler* self, int code,
                   const char* message, const char* metadata);
  void (*release)(struct ArrowAsyncDeviceStreamHandler* self);
  struct ArrowAsyncProducer* producer;
  void* private_data;
};

#endif  // ARROW_C_ASYNC_STREAM_INTERFACE

struct InteropProxy;

// Install the proxy callbacks on outer (the handler given to the .NET producer)
// and return the proxy. Pass InteropProxy_Inner to arrow-go's consumer.
struct InteropProxy* InteropProxy_Create(struct ArrowAsyncDeviceStreamHandler* outer);

struct ArrowAsyncDeviceStreamHandler* InteropProxy_Inner(struct InteropProxy* proxy);

// Whether the producer has released the outer handler.
int InteropProxy_IsReleased(struct InteropProxy* proxy);

// Free the proxy. Only call once the outer handler has been released and
// arrow-go's consumer goroutine has exited.
void InteropProxy_Free(struct InteropProxy* proxy);
