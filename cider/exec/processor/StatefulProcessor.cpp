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

#include "StatefulProcessor.h"

namespace cider::processor {

StatefulProcessor::StatefulProcessor(const ::substrait::Plan& plan,
                                     const BatchProcessorContextPtr& context)
    : BatchProcessor(plan, context) {}

void StatefulProcessor::processNextBatch(std::shared_ptr<CiderBatch> batch) {
  // TODO: process batch through jitLib API
}

std::shared_ptr<CiderBatch> StatefulProcessor::getResult() {
  return nullptr;
}

BatchProcessorState StatefulProcessor::getState() {
  return state_;
}

}  // namespace cider::processor
