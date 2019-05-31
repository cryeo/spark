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

package org.apache.spark.sql

import scala.util.Random

import org.scalatest.Matchers.the

import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData.DecimalData
import org.apache.spark.sql.types.DecimalType

case class Fact(date: Int, hour: Int, minute: Int, room_name: String, temp: Double)

class DataFrameAggregateSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("SPARK-27425: count_if function") {
    def checkError(df: => DataFrame): Unit = {
      val thrownException = the [AnalysisException] thrownBy df.queryExecution.analyzed
      assert(thrownException.message.contains("function count_if requires boolean type"))
    }

    withTempView("tempView") {
      Seq(("a", None), ("a", Some(1)), ("a", Some(2)), ("a", Some(3)),
        ("b", None), ("b", Some(4)), ("b", Some(5)), ("b", Some(6)))
        .toDF("x", "y")
        .createOrReplaceTempView("tempView")

      checkAnswer(
        sql("SELECT COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) FROM tempView"),
        Row(3L, 3L, 2L))

      checkAnswer(
        sql("SELECT x, COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) " +
            "FROM tempView GROUP BY x"),
        Row("a", 1L, 2L, 1L) :: Row("b", 2L, 1L, 1L) :: Nil)

      checkError(sql("SELECT COUNT_IF(x) FROM tempView"))
    }
  }
}
