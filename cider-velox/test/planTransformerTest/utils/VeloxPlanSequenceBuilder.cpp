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

#include "VeloxPlanSequenceBuilder.h"

namespace facebook::velox::plugin::plantransformer::test {

VeloxPlanSequenceBuilder& VeloxPlanSequenceBuilder::filter() {
  core::TypedExprPtr filter;
  planNode_ = std::make_shared<core::FilterNode>(nextPlanNodeId(), filter, planNode_);
  return *this;
}

VeloxPlanSequenceBuilder& VeloxPlanSequenceBuilder::proj() {
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> projections;
  planNode_ = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(), names, projections, planNode_);
  return *this;
}
VeloxPlanSequenceBuilder& VeloxPlanSequenceBuilder::partialAgg() {
  const std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
  const std::vector<core::FieldAccessTypedExprPtr> preGroupedKeys;
  const std::vector<std::string> aggregateNames;
  const std::vector<core::CallTypedExprPtr> aggregates;
  const std::vector<core::FieldAccessTypedExprPtr> aggregateMasks;
  bool ignoreNullKeys;
  planNode_ =
      std::make_shared<core::AggregationNode>(nextPlanNodeId(),
                                              core::AggregationNode::Step::kPartial,
                                              groupingKeys,
                                              preGroupedKeys,
                                              aggregateNames,
                                              aggregates,
                                              aggregateMasks,
                                              ignoreNullKeys,
                                              planNode_);
  return *this;
}

const VeloxPlanNodePtr& VeloxPlanSequenceBuilder::planNode() {
  return planNode_;
}

std::string VeloxPlanSequenceBuilder::nextPlanNodeId() {
  return fmt::format("{}", planNodeId_++);
}
}  // namespace facebook::velox::plugin::plantransformer::test
