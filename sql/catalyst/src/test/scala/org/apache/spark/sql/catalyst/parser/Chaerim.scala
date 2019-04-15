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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.types.DataType

// scalastyle:off println
object Chaerim extends App {
  def parse(sql: String): DataType = CatalystSqlParser.parseDataType(sql)

  println(CatalystSqlParser.parsePlan(
    """
      |SELECT
      |  COUNT(1) FILTER (WHERE b % 2 = 0),
      |  SUM(b) FILTER (WHERE b % 2 = 0)
      |FROM
      |  VALUES
      |  (1, 2),
      |  (1, 3),
      |  (1, 4),
      |  (2, 1),
      |  (2, 2),
      |  (2, 3)
      |  AS tbl(a, b)
      |GROUP BY
      |  a
    """.stripMargin))

  println(CatalystSqlParser.parsePlan(
    """
      |SELECT
      |  COUNT(1),
      |  SUM(b)
      |FROM
      |  VALUES
      |  (1, 2),
      |  (1, 3),
      |  (1, 4),
      |  (2, 1),
      |  (2, 2),
      |  (2, 3)
      |  AS tbl(a, b)
      |GROUP BY
      |  a
    """.stripMargin))

  println(CatalystSqlParser.parsePlan(
    """
      |SELECT
      |  COUNT(1) FILTER (WHERE b % 2 = 0),
      |  SUM(b) FILTER (WHERE b % 2 = 0)
      |FROM
      |  VALUES
      |  (1, 2),
      |  (1, 3),
      |  (1, 4),
      |  (2, 1),
      |  (2, 2),
      |  (2, 3)
      |  AS tbl(a, b)
    """.stripMargin))
}
// scalastyle:on println
