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
#include "exec/nextgen/operators/FilterNode.h"

#include "exec/nextgen/jitlib/JITLib.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;

TranslatorPtr FilterNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<FilterTranslator>(shared_from_this(), succ);
}

void FilterTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void FilterTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  func->createIfBuilder()
      ->condition([&]() {
        auto bool_init = func->createVariable(JITTypeTag::BOOL, "bool_init");
        bool_init = func->createConstant(JITTypeTag::BOOL, true);
        auto&& [expr_type, exprs] = node_->getOutputExprs();
        for (const auto& expr : exprs) {
          utils::FixSizeJITExprValue cond(expr->codegen(*func));
          bool_init = bool_init && cond.getValue();
          if (cond.isNullable()) {
            bool_init = bool_init && !cond.getNull();
          }
        }
        return bool_init;
      })
      ->ifTrue([&]() { successor_->consume(context); })
      ->build();
}
}  // namespace cider::exec::nextgen::operators
