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

#include <folly/init/Init.h>

#include "../CiderOperatorTestBase.h"
#include "../CiderPlanBuilder.h"
#include "PlanTranformerIncludes.h"
#include "utils/FilterProjectSwapTransformer.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using facebook::velox::test::BatchMaker;

namespace facebook::velox::plugin::plantransformer::test {
class FilterProjectSwapTest : public PlanTransformerTestBase {
 public:
  FilterProjectSwapTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<ProjectFilterPattern>(),
        std::make_shared<ProjcetFilterSwapRewriter>());
    setTransformerFactory(transformerFactory);
  }

 protected:
  std::vector<std::string> projections_ = {"c0", "c1", "c0 + 2"};
  std::string filter_ = "c0 > 2 ";
  std::vector<std::string> aggs_ = {"SUM(c1)"};
  RowTypePtr rowType_{ROW({"c0", "c1"}, {BIGINT(), INTEGER()})};
};

TEST_F(FilterProjectSwapTest, noNeedToSwap) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter(filter_)
                                 .project(projections_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();
  std::cout << planPtr->toString(true, true);

  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planPtr));
}

TEST_F(FilterProjectSwapTest, singelBranchSwap) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .project(projections_)
                                 .filter(filter_)
                                 .project(projections_)
                                 .filter(filter_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr = PlanBuilder()
                                     .values(generateTestBatch(rowType_, false))
                                     .filter(filter_)
                                     .project(projections_)
                                     .filter(filter_)
                                     .project(projections_)
                                     .partialAggregation({}, aggs_)
                                     .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

// TEST_F(FilterProjectSwapTest, singelBranchSwap2) {
//   VeloxPlanBuilder transformPlanBuilder;
//   VeloxPlanNodePtr planPtr =
//       transformPlanBuilder.proj().proj().filter().proj().filter().planNode();
//   VeloxPlanBuilder expectedPlanBuilder;
//   VeloxPlanNodePtr expectedPtr =
//       expectedPlanBuilder.proj().filter().proj().filter().proj().planNode();
//   VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
//   EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
// }

// TEST_F(FilterProjectSwapTest, MultiBranchesSwap) {
//   VeloxPlanBuilder planRightBranchBuilder;
//   VeloxPlanNodePtr planRightPtr =
//       planRightBranchBuilder.proj().proj().filter().proj().filter().planNode();
//   VeloxPlanBuilder planLeftBranchBuilder;
//   VeloxPlanNodePtr planLeftPtr = planLeftBranchBuilder.proj()
//                                      .filter()
//                                      .hashjoin(planRightPtr)
//                                      .proj()
//                                      .filter()
//                                      .partialAgg()
//                                      .planNode();
//   VeloxPlanBuilder expectedRightBranchBuilder;
//   VeloxPlanNodePtr expectedRightPtr =
//       expectedRightBranchBuilder.proj().filter().proj().filter().proj().planNode();
//   VeloxPlanBuilder expectedLeftBranchBuilder;
//   VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter()
//                                          .proj()
//                                          .hashjoin(expectedRightPtr)
//                                          .filter()
//                                          .proj()
//                                          .partialAgg()
//                                          .planNode();
//   VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
//   EXPECT_TRUE(compareWithExpected(resultPtr, expectedLeftPtr));
// }

}  // namespace facebook::velox::plugin::plantransformer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}