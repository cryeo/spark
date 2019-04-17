/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType}

case class AggregateFilter(filter: Expression) extends Expression {
  override def children: Seq[Expression] = filter :: Nil

  override def dataType: DataType = filter.dataType

  override def nullable: Boolean = filter.nullable

  override def foldable: Boolean = filter.foldable

  override def eval(input: InternalRow): Any = filter.eval(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    filter.genCode(ctx)

  override def checkInputDataTypes(): TypeCheckResult = filter.dataType match {
    case BooleanType =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(
        s"filter requires boolean type, not ${filter.dataType.catalogString}")
  }

  override def sql: String = s"FILTER (WHERE ${filter.sql})"
}

case class FilteredAggregateExpression(
    aggregateFunction: Expression,
    aggregateFilter: Expression) extends Expression with Unevaluable {
  override def children: Seq[Expression] = aggregateFunction :: aggregateFilter :: Nil

  override def nullable: Boolean = aggregateFunction.nullable

  override def foldable: Boolean = aggregateFunction.foldable

  override def dataType: DataType = aggregateFunction.dataType

  override def checkInputDataTypes(): TypeCheckResult = aggregateFunction match {
    case _: AggregateWindowFunction =>
      TypeCheckResult.TypeCheckFailure("window function is currently not supported")
    case _: AggregateExpression =>
      TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure("only aggregate function is supported")
  }

  override def toString: String = s"$aggregateFunction $aggregateFilter"

  override def sql: String = s"${aggregateFunction.sql} ${aggregateFilter.sql}"

  private def filter: Boolean = aggregateFilter.eval().asInstanceOf[Boolean]
}
