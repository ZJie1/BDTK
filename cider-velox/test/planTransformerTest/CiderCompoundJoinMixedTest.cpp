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
class CiderCompoundJoinMixedTest : public PlanTransformerTestBase {
 public:
  CiderCompoundJoinMixedTest() {
    auto transformerFactory =
        PlanTransformerFactory()
            .registerPattern(std::make_shared<plantransformer::CompoundPattern>(),
                             std::make_shared<CiderPatternTestNodeRewriter>())
            .registerPattern(std::make_shared<plantransformer::LeftDeepJoinPattern>(),
                             std::make_shared<CiderPatternTestNodeRewriter>())
            .registerPattern(std::make_shared<plantransformer::FilterPattern>(),
                             std::make_shared<CiderPatternTestNodeRewriter>());
    setTransformerFactory(transformerFactory);
  }

 protected:
  std::vector<std::string> projections_ = {"c0", "c1", "c0 + 2"};
  std::string filter_ = "c0 > 2 ";
  std::vector<std::string> aggs_ = {"SUM(c1)"};
  RowTypePtr rowType_{ROW({"c0", "c1"}, {BIGINT(), INTEGER()})};
  RowTypePtr rowTypeLeft_{ROW({"c2", "c3"}, {BIGINT(), INTEGER()})};

  VeloxPlanNodePtr planRight1Ptr_ =
      getSingleProjectNode(rowType_, {"c0 as u_c0", "c1 as u_c1"});

  VeloxPlanNodePtr planRight2Ptr_ = getSingleFilterNode(rowType_, filter_);
};

TEST_F(CiderCompoundJoinMixedTest, filterOnly) {
  VeloxPlanNodePtr resultPtr =
      getTransformer(getSingleFilterNode(rowType_, filter_))->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, getCiderExpectedPtr(rowType_)));
}

TEST_F(CiderCompoundJoinMixedTest, filterAgg) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter(filter_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();

  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowType_, false))
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .partialAggregation({}, aggs_)
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundJoinMixedTest, filterProjectAgg) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter(filter_)
                                 .project(projections_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();

  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, getCiderExpectedPtr(rowType_)));
}

TEST_F(CiderCompoundJoinMixedTest, SingleJoinMultiCompoundNodes) {
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

  VeloxPlanNodePtr expectedRightPtr =
      PlanBuilder()
          .values(generateTestBatch(rowType_, false))
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();

  VeloxPlanNodeVec joinSrcVec{getCiderExpectedPtr(rowTypeLeft_), expectedRightPtr};

  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
          })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundJoinMixedTest, DontMergeMultiJoinNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr_, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr_, "", {"c2", "c3", "c1"})
          .filter("c1 > 2")
          .project({"c2", "c3", "c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec{getCiderExpectedPtr(rowTypeLeft_),
                              getCiderExpectedPtr(rowType_),
                              getCiderExpectedPtr(rowType_)};

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_FALSE(
      compareWithExpected(resultPtr, getCiderExpectedPtr(rowTypeLeft_, joinSrcVec)));
}

TEST_F(CiderCompoundJoinMixedTest, MultiJoinNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr_, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr_, "", {"c2", "c3", "c1"})
          .filter("c1 > 2")
          .project({"c2", "c3", "c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec1{getCiderExpectedPtr(rowTypeLeft_),
                               getCiderExpectedPtr(rowType_)};

  VeloxPlanNodeVec joinSrcVec2{getCiderExpectedPtr(rowTypeLeft_, joinSrcVec1),
                               getCiderExpectedPtr(rowType_)};

  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, joinSrcVec2);
          })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundJoinMixedTest, MultiSeperatedJoinNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr_, "", {"c2", "c3", "u_c1"})
          .filter("u_c1 > 4")
          .project({"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr_, "", {"c2", "c3", "c1"})
          .filter("c1 > 2")
          .project({"c2", "c3", "c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodeVec join1SrcVec{getCiderExpectedPtr(rowTypeLeft_),
                               getCiderExpectedPtr(rowType_)};
  // hashJoin + filter match LeftDeepJoinPattern and project match CompoundPattern.
  VeloxPlanNodePtr expectedJoin2Left =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode([&join1SrcVec](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, join1SrcVec);
          })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();

  VeloxPlanNodeVec join2SrcVec{expectedJoin2Left, getCiderExpectedPtr(rowType_)};

  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode([&join2SrcVec](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, join2SrcVec);
          })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();

  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

}  // namespace facebook::velox::plugin::plantransformer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
