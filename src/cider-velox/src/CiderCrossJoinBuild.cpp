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

#include "CiderCrossJoinBuild.h"
#include "Allocator.h"
#include "velox/exec/Task.h"
// #include "velox/vector/arrow/Abi.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

void CiderCrossJoinBridge::setData(CiderCrossBuildData data) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(!data_.has_value(), "setData may be called only once");
    this->data_ = CiderCrossBuildData(std::move(data));

    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<CiderCrossBuildData> CiderCrossJoinBridge::hasDataOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!cancelled_, "Getting data after the build side is aborted");
  if (data_.has_value()) {
    return data_;
  }
  promises_.emplace_back("CiderCrossJoinBridge::getCrossBuildData");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

CiderCrossJoinBuild::CiderCrossJoinBuild(int32_t operatorId,
                                         exec::DriverCtx* driverCtx,
                                         std::shared_ptr<const CiderPlanNode> joinNode)
    : Operator(driverCtx, nullptr, operatorId, joinNode->id(), "CiderCrossJoinBuild")
    , allocator_(std::make_shared<PoolAllocator>(operatorCtx_->pool())) {
//  const auto& joinRel = joinNode->getSubstraitPlan().relations(0).root().input().join();
//  auto context = std::make_shared<CiderJoinHashTableBuildContext>(allocator_);
//  joinHashTableBuilder_ =
//      cider::exec::processor::makeJoinHashTableBuilder(joinRel, context);
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  joinBridge_ = std::dynamic_pointer_cast<CiderCrossJoinBridge>(joinBridge);
}

void CiderCrossJoinBuild::addInput(RowVectorPtr input){
  for (size_t i = 0; i < input->childrenSize(); i++) {
    input->childAt(i)->mutableRawNulls();
  }
  ArrowArray* inputArrowArray = CiderBatchUtils::allocateArrowArray();
  exportToArrow(input_, *inputArrowArray);
  ArrowSchema* inputArrowSchema = CiderBatchUtils::allocateArrowSchema();
  exportToArrow(input_, *inputArrowSchema);

  auto inBatch =
      CiderBatchUtils::createCiderBatch(allocator_, inputArrowSchema, inputArrowArray);
  //joinHashTableBuilder_->appendBatch(std::move(inBatch));

  data_.data = inputArrowArray;
  data_.schema = inputArrowSchema;
}



}  // namespace facebook::velox::plugin
