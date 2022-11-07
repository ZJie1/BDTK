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
#include "CiderPlanTransformerIncludes.h"

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;
class CiderLeftDeepJoinPatternTest : public PlanTransformerTestBase {
 public:
  CiderLeftDeepJoinPatternTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<plantransformer::LeftDeepJoinPattern>(),
        std::make_shared<CiderPatternTestNodeRewriter>());
    setTransformerFactory(transformerFactory);
  }

 protected:
  RowTypePtr rowType_{ROW({"c0", "c1"}, {BIGINT(), INTEGER()})};
  RowTypePtr rowTypeLeft_{ROW({"c2", "c3"}, {BIGINT(), INTEGER()})};
};
TEST_F(CiderLeftDeepJoinPatternTest, JoinCompoundNodes) {
  VeloxPlanNodePtr planRightPtr = PlanBuilder()
                                      .values(generateTestBatch(rowType_, false))
                                      .project({"c0 as u_c0", "c1 as u_c1"})
                                      .filter("u_c0 > 1")
                                      .project({"u_c0", "u_c1"})
                                      .planNode();

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr, "", {"c2", "c3", "u_c1"})
          .filter("u_c1 > 2")
          .project({"c2", "c3", "u_c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr = PlanBuilder()
                                         .values(generateTestBatch(rowTypeLeft_, false))
                                         .filter("c2 > 3")
                                         .project({"c2", "c3"})
                                         .planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, planRightPtr};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinWithoutCompoundNodes) {
  VeloxPlanNodePtr planRightPtr = PlanBuilder()
                                      .values(generateTestBatch(rowType_, false))
                                      .project({"c0 as u_c0", "c1 as u_c1"})
                                      .filter("u_c0 > 1")
                                      .project({"u_c0", "u_c1"})
                                      .planNode();

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr, "", {"c2", "c3", "u_c1"})
          .filter("c3 > 2")
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr = PlanBuilder()
                                         .values(generateTestBatch(rowTypeLeft_, false))
                                         .filter("c2 > 3")
                                         .project({"c2", "c3"})
                                         .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, planRightPtr};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .filter("c3 > 2")
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, NotAcceptMultiJoinNodes) {
  VeloxPlanNodePtr planRight1Ptr = PlanBuilder()
                                       .values(generateTestBatch(rowType_, false))
                                       .project({"c0 as u_c0", "c1 as u_c1"})
                                       .planNode();

  VeloxPlanNodePtr planRight2Ptr = PlanBuilder()
                                       .values(generateTestBatch(rowType_, false))
                                       .filter("c0 > 1")
                                       .planNode();

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr, "", {"c2", "c3", "c1"})
          .filter("c1 > 2")
          .project({"c2", "c3", "c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr = PlanBuilder()
                                         .values(generateTestBatch(rowTypeLeft_, false))
                                         .filter("c2 > 3")
                                         .planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, planRight1Ptr, planRight2Ptr};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_FALSE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinNodeAsRoot) {
  VeloxPlanNodePtr planRightPtr = PlanBuilder()
                                      .values(generateTestBatch(rowType_, false))
                                      .project({"c0 as u_c0", "c1 as u_c1"})
                                      .filter("u_c0 > 1")
                                      .project({"u_c0", "u_c1"})
                                      .planNode();

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr, "", {"c2", "c3", "u_c1"})
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr = PlanBuilder()
                                         .values(generateTestBatch(rowTypeLeft_, false))
                                         .filter("c2 > 3")
                                         .project({"c2", "c3"})
                                         .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, planRightPtr};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, NotAcceptMultiJoinNodesAsRoot) {
  VeloxPlanNodePtr planRight1Ptr = PlanBuilder()
                                       .values(generateTestBatch(rowType_, false))
                                       .project({"c0 as u_c0", "c1 as u_c1"})
                                       .planNode();

  VeloxPlanNodePtr planRight2Ptr = PlanBuilder()
                                       .values(generateTestBatch(rowType_, false))
                                       .filter("c0 > 1")
                                       .planNode();

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr, "", {"c2", "c3", "c1"})
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr = PlanBuilder()
                                         .values(generateTestBatch(rowTypeLeft_, false))
                                         .filter("c2 > 3")
                                         .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, planRight1Ptr, planRight2Ptr};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_FALSE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, MultiJoinNodes) {
  VeloxPlanNodePtr planRight1Ptr = PlanBuilder()
                                       .values(generateTestBatch(rowType_, false))
                                       .project({"c0 as u_c0", "c1 as u_c1"})
                                       .planNode();

  VeloxPlanNodePtr planRight2Ptr = PlanBuilder()
                                       .values(generateTestBatch(rowType_, false))
                                       .filter("c0 > 1")
                                       .planNode();

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr, "", {"c2", "c3", "c1"})
          .filter("c1 > 2")
          .project({"c2", "c3", "c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr = PlanBuilder()
                                         .values(generateTestBatch(rowTypeLeft_, false))
                                         .filter("c2 > 3")
                                         .planNode();
  VeloxPlanNodeVec joinSrcVec1{expectedLeftPtr, planRight1Ptr};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode([&joinSrcVec1](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, joinSrcVec1);
          })
          .planNode();

  VeloxPlanNodeVec joinSrcVec2{expectedJoinPtr, planRight2Ptr};

  expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode([&joinSrcVec2](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, joinSrcVec2);
          })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}
}  // namespace facebook::velox::plugin::plantransformer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}