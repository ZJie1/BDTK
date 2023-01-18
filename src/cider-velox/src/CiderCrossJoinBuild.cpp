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

#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#define CIDER_BATCH_PROCESSOR_CONTEXT_H
#include "velox/vector/arrow/Abi.h"
#endif

#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

void CiderCrossJoinBridge::setData(Batch data) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(!data_.has_value(), "setData may be called only once");
    this->data_ = std::move(data);

    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<Batch> CiderCrossJoinBridge::hasDataOrFuture(ContinueFuture* future) {
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
  //  const auto& joinRel =
  //  joinNode->getSubstraitPlan().relations(0).root().input().join(); auto context =
  //  std::make_shared<CiderJoinHashTableBuildContext>(allocator_); joinHashTableBuilder_
  //  =
  //      cider::exec::processor::makeJoinHashTableBuilder(joinRel, context);
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  joinBridge_ = std::dynamic_pointer_cast<CiderCrossJoinBridge>(joinBridge);
}

void CiderCrossJoinBuild::addInput(RowVectorPtr input) {
  for (size_t i = 0; i < input->childrenSize(); i++) {
    input->childAt(i)->mutableRawNulls();
  }
  ArrowArray* inputArrowArray = CiderBatchUtils::allocateArrowArray();
  exportToArrow(input_, *inputArrowArray);
  ArrowSchema* inputArrowSchema = CiderBatchUtils::allocateArrowSchema();
  exportToArrow(input_, *inputArrowSchema);

  cider::exec::nextgen::context::Batch inBatch(*inputArrowSchema, *inputArrowArray);
  //  auto inBatch =
  //      CiderBatchUtils::createCiderBatch(allocator_, inputArrowSchema,
  //      inputArrowArray);
  //  // joinHashTableBuilder_->appendBatch(std::move(inBatch));

  data_ = std::move(inBatch);
}

void CiderCrossJoinBuild::noMoreInput() {
  Operator::noMoreInput();
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;
  // The last Driver to hit CrossJoinBuild::finish gathers the data from
  // all build Drivers and hands it over to the probe side. At this
  // point all build Drivers are continued and will free their
  // state. allPeersFinished is true only for the last Driver of the
  // build pipeline.
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return;
  }

  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    auto* build = dynamic_cast<CiderCrossJoinBuild*>(op);
    VELOX_CHECK(build);
    // TODO: append the data from peers into the current data.
    // data_.data;
    // data_.insert(data_.begin(), build->data_.begin(), build->data_.end());
  }

  // Realize the promises so that the other Drivers (which were not
  // the last to finish) can continue from the barrier and finish.
  peers.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }

  joinBridge_->setData(std::move(data_));
}

exec::BlockingReason CiderCrossJoinBuild::isBlocked(
    facebook::velox::ContinueFuture* future) {
  if (!future_.valid()) {
    return exec::BlockingReason::kNotBlocked;
  }
  *future = std::move(future_);
  return exec::BlockingReason::kWaitForJoinBuild;
}

bool CiderCrossJoinBuild::isFinished() {
  return !future_.valid() && noMoreInput_;
}

}  // namespace facebook::velox::plugin
