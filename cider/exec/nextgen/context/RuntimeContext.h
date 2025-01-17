/*
 * Copyright (c) 2022 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
#define NEXTGEN_CONTEXT_RUNTIMECONTEXT_H

#include "exec/nextgen/context/Batch.h"
#include "exec/nextgen/context/CodegenContext.h"

namespace cider::exec::nextgen::context {
class RuntimeContext {
 public:
  RuntimeContext(int64_t ctx_num) : runtime_ctx_pointers_(ctx_num, nullptr) {}

  size_t getContextItemNum() const { return runtime_ctx_pointers_.size(); }

  void* getContextItem(size_t id) { return runtime_ctx_pointers_[id]; }

  void addBatch(const CodegenContext::BatchDescriptorPtr& descriptor);

  void instantiate(const CiderAllocatorPtr& allocator);

  // TBD: Currently, last batch would be output batch under all known scenarios.
  Batch* getOutputBatch() {
    if (batch_holder_.empty()) {
      return nullptr;
    }
    return batch_holder_.back().second.get();
  }

 private:
  std::vector<void*> runtime_ctx_pointers_;
  std::vector<std::pair<CodegenContext::BatchDescriptorPtr, BatchPtr>> batch_holder_;
};

using RuntimeCtxPtr = std::unique_ptr<RuntimeContext>;
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
