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

// See proxy.h for why this exists.

#include "proxy.h"

#include <stdlib.h>
#include <string.h>

struct InteropProxy {
  struct ArrowAsyncDeviceStreamHandler* outer;
  struct ArrowAsyncDeviceStreamHandler inner;
  struct ArrowAsyncProducer producer;

  // Guards real_producer and released. Held while forwarding request/cancel,
  // so release can't proceed (and the real producer be freed) mid-call.
  // released means outer_release has finished with the proxy, not just begun. A
  // spinlock keeps this free of threading-library dependencies; critical
  // sections are short because request/cancel must not block or call back.
  volatile char lock;
  struct ArrowAsyncProducer* real_producer;
  int released;
};

static void proxy_lock(struct InteropProxy* p) {
  while (__atomic_test_and_set(&p->lock, __ATOMIC_ACQUIRE)) {
  }
}

static void proxy_unlock(struct InteropProxy* p) {
  __atomic_clear(&p->lock, __ATOMIC_RELEASE);
}

static struct InteropProxy* from_outer(struct ArrowAsyncDeviceStreamHandler* self) {
  return (struct InteropProxy*)self->private_data;
}

// ---- proxy producer, called by arrow-go ----

static void proxy_request(struct ArrowAsyncProducer* self, int64_t n) {
  struct InteropProxy* p = (struct InteropProxy*)self->private_data;
  proxy_lock(p);
  // real_producer is cleared as soon as release starts.
  if (p->real_producer != NULL) {
    p->real_producer->request(p->real_producer, n);
  }
  proxy_unlock(p);
}

static void proxy_cancel(struct ArrowAsyncProducer* self) {
  struct InteropProxy* p = (struct InteropProxy*)self->private_data;
  proxy_lock(p);
  if (p->real_producer != NULL) {
    p->real_producer->cancel(p->real_producer);
  }
  proxy_unlock(p);
}

// ---- outer handler, called by the .NET producer; forwards to arrow-go ----

static int outer_on_schema(struct ArrowAsyncDeviceStreamHandler* self,
                           struct ArrowSchema* schema) {
  struct InteropProxy* p = from_outer(self);

  proxy_lock(p);
  p->real_producer = self->producer;
  proxy_unlock(p);

  p->producer.device_type = self->producer->device_type;
  p->producer.additional_metadata = self->producer->additional_metadata;
  p->inner.producer = &p->producer;

  // Not under the lock: arrow-go calls request() synchronously from on_schema.
  return p->inner.on_schema(&p->inner, schema);
}

static int outer_on_next_task(struct ArrowAsyncDeviceStreamHandler* self,
                              struct ArrowAsyncTask* task, const char* metadata) {
  struct InteropProxy* p = from_outer(self);
  return p->inner.on_next_task(&p->inner, task, metadata);
}

static void outer_on_error(struct ArrowAsyncDeviceStreamHandler* self, int code,
                           const char* message, const char* metadata) {
  struct InteropProxy* p = from_outer(self);
  p->inner.on_error(&p->inner, code, message, metadata);
}

static void outer_release(struct ArrowAsyncDeviceStreamHandler* self) {
  struct InteropProxy* p = from_outer(self);

  // The real producer may be freed once this returns: stop forwarding to it now.
  proxy_lock(p);
  p->real_producer = NULL;
  proxy_unlock(p);

  // arrow-go keeps writing to the inner handler (which lives in the proxy) after
  // its task goroutine may have exited, so the proxy must not be reported as
  // released until this returns.
  if (p->inner.release != NULL) {
    p->inner.release(&p->inner);
  }

  self->private_data = NULL;

  // Publish completion only after the last access to each struct: once released
  // is set, ArrowGoAsync_ConsumerFree may free the proxy, and once self->release
  // is cleared, the test may free the outer handler.
  proxy_lock(p);
  p->released = 1;
  proxy_unlock(p);
  __atomic_store_n(&self->release, NULL, __ATOMIC_RELEASE);
}

// ---- lifetime ----

struct InteropProxy* InteropProxy_Create(struct ArrowAsyncDeviceStreamHandler* outer) {
  struct InteropProxy* p = (struct InteropProxy*)calloc(1, sizeof(struct InteropProxy));
  if (p == NULL) {
    return NULL;
  }

  p->outer = outer;
  p->producer.request = proxy_request;
  p->producer.cancel = proxy_cancel;
  p->producer.private_data = p;

  memset(outer, 0, sizeof(*outer));
  outer->on_schema = outer_on_schema;
  outer->on_next_task = outer_on_next_task;
  outer->on_error = outer_on_error;
  outer->release = outer_release;
  outer->private_data = p;
  return p;
}

struct ArrowAsyncDeviceStreamHandler* InteropProxy_Inner(struct InteropProxy* proxy) {
  return &proxy->inner;
}

int InteropProxy_IsReleased(struct InteropProxy* proxy) {
  proxy_lock(proxy);
  int released = proxy->released;
  proxy_unlock(proxy);
  return released;
}

void InteropProxy_Free(struct InteropProxy* proxy) { free(proxy); }
