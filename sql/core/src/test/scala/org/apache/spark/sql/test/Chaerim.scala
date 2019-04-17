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

package org.apache.spark.sql.test

import java.net.URI

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// scalastyle:off println
class ChaerimSuite extends QueryTest with SharedSQLContext {
  val relation = LocalRelation(
    AttributeReference("a", IntegerType)() ::
      AttributeReference("b", IntegerType)() ::
      AttributeReference("c", BooleanType)() :: Nil,
    InternalRow(1, 1, true) ::
      InternalRow(1, 2, false) ::
      InternalRow(1, 3, true) ::
      InternalRow(2, 1, false) :: Nil
  )

  def makeAnalyzer(caseSensitive: Boolean): Analyzer = {
    val conf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> caseSensitive,
      SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_LEVEL -> "debug")
    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, conf)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    catalog.createTempView("base", relation, overrideIfExists = true)
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = EliminateSubqueryAliases :: Nil
    }
  }

  test("asdsad") {
    val query =
      """
        |SELECT
        |  COUNT(1) FILTER (WHERE b % 2 = 1)
        |FROM
        |  base
      """.stripMargin

    val parsedPlan = CatalystSqlParser.parsePlan(query)
    println(parsedPlan)

    val analyzer = makeAnalyzer(false)
    val analyzedPlan = analyzer.execute(parsedPlan)
    println(analyzedPlan)

    val optimizedPlan = SimpleTestOptimizer.execute(analyzedPlan)
    println(optimizedPlan)

    val physicalPlan = optimizedPlan.queryExecution.executedPlan
    println(physicalPlan)

    val rdd = physicalPlan.execute()
    println(rdd.collect().toList)
  }
}
// scalastyle:on println
