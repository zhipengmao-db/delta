/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.{FileNotFoundException, PrintWriter}

import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedAsIdentityType}
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.apache.spark.util.Utils

// Test command that should be allowed and disallowed on IDENTITY columns.
trait IdentityColumnAdmissionSuiteBase
  extends IdentityColumnTestUtils {

  import testImplicits._

  protected val tblName = "identity_admission_test"

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key, "true")
  }

  test("alter table change column type") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      keyword <- Seq("ALTER", "CHANGE")
      targetType <- Seq(IntegerType, DoubleType)
    } {
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        // Type changes that aren't upcast are rejected early during analysis by Spark,
        // while upcasts are rejected in Delta when altering data type of an identity column.
        if (Cast.canUpCast(LongType, targetType)) {
          val ex = intercept[DeltaAnalysisException] {
              sql(s"ALTER TABLE $tblName $keyword COLUMN id TYPE ${targetType.sql}")
            }
          assert(ex.getErrorClass === "DELTA_IDENTITY_COLUMNS_ALTER_COLUMN_NOT_SUPPORTED")
        } else {
          val ex = intercept[AnalysisException] {
              sql(s"ALTER TABLE $tblName $keyword COLUMN id TYPE ${targetType.sql}")
          }
          assert(ex.getErrorClass === "NOT_SUPPORTED_CHANGE_COLUMN")
        }
      }
    }
  }

  test("alter table change column comment") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      keyword <- Seq("ALTER", "CHANGE")
    } {
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        sql(s"ALTER TABLE $tblName $keyword COLUMN id COMMENT 'comment'")
      }
    }
  }

  test("alter table replace columns") {
    for (generatedAsIdentityType <- GeneratedAsIdentityType.values) {
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        val ex = intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tblName REPLACE COLUMNS (id BIGINT, value INT)")
        }
        assert(ex.getMessage.contains("ALTER TABLE REPLACE COLUMNS is not supported"))
      }
    }
  }

  test("create table partitioned by identity column") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
    } {
      withTable(tblName) {
        val ex1 = intercept[DeltaAnalysisException] {
          createTable(
            tblName,
            Seq(
              IdentityColumnSpec(generatedAsIdentityType),
              TestColumnSpec("value1", dataType = IntegerType),
              TestColumnSpec("value2", dataType = DoubleType)
            ),
            partitionedBy = Seq("id")
          )
        }
        assert(ex1.getMessage.contains("PARTITIONED BY IDENTITY column"))
        val ex2 = intercept[DeltaAnalysisException] {
          createTable(
            tblName,
            Seq(
              IdentityColumnSpec(generatedAsIdentityType),
              TestColumnSpec("value1", dataType = IntegerType),
              TestColumnSpec("value2", dataType = DoubleType)
            ),
            partitionedBy = Seq("id", "value1")
          )
        }
        assert(ex2.getMessage.contains("PARTITIONED BY IDENTITY column"))
      }
    }
  }

  test("replace with table partitioned by identity column") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
    } {
      withTable(tblName) {
        // First create a table with no identity column and no partitions.
        createTable(
          tblName,
          Seq(
            TestColumnSpec("id", dataType = LongType),
            TestColumnSpec("value1", dataType = IntegerType),
            TestColumnSpec("value2", dataType = DoubleType)
          )
        )
        // CREATE OR REPLACE should not allow a table using identity column with partition.
        val ex1 = intercept[DeltaAnalysisException] {
          createOrReplaceTable(
            tblName,
            Seq(
              IdentityColumnSpec(generatedAsIdentityType),
              TestColumnSpec("value1", dataType = IntegerType),
              TestColumnSpec("value2", dataType = DoubleType)
            ),
            partitionedBy = Seq("id")
          )
        }
        assert(ex1.getMessage.contains("PARTITIONED BY IDENTITY column"))
        // REPLACE should also not allow a table using identity column as partition.
        val ex2 = intercept[DeltaAnalysisException] {
          replaceTable(
            tblName,
            Seq(
              IdentityColumnSpec(generatedAsIdentityType),
              TestColumnSpec("value1", dataType = IntegerType),
              TestColumnSpec("value2", dataType = DoubleType)
            ),
            partitionedBy = Seq("id", "value1")
          )
        }
        assert(ex2.getMessage.contains("PARTITIONED BY IDENTITY column"))
      }
    }
  }

  test("CTAS does not inherit IDENTITY column") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
    } {
      val ctasTblName = "ctasTblName"
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        withTable(ctasTblName) {
          sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")
          sql(
            s"""
               |CREATE TABLE $ctasTblName USING delta AS SELECT * FROM $tblName
               |""".stripMargin)
          val dl = DeltaLog.forTable(spark, TableIdentifier(ctasTblName))
          assert(!dl.snapshot.metadata.schemaString.contains(DeltaSourceUtils.IDENTITY_INFO_START))
        }
      }
    }
  }

  test("insert generated always as") {
    withIdentityColumnTable(GeneratedAlways, tblName) {
      // Test SQLs.
      val blockedStmts = Seq(
        s"INSERT INTO $tblName VALUES (1,1)",
        s"INSERT INTO $tblName (value, id) VALUES (1,1)",
        s"INSERT OVERWRITE $tblName VALUES (1,1)",
        s"INSERT OVERWRITE $tblName (value, id) VALUES (1,1)"
      )
      for (stmt <- blockedStmts) {
        val ex = intercept[DeltaAnalysisException](sql(stmt))
        assert(ex.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))
      }

      // Test DataFrame V1 and V2 API.
      val df = (1 to 10).map(v => (v.toLong, v)).toDF("id", "value")

      val path = DeltaLog.forTable(spark, TableIdentifier(tblName)).dataPath.toString
      val exV1 =
        intercept[DeltaAnalysisException](df.write.format("delta").mode("append").save(path))
      assert(exV1.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))

      val exV2 = intercept[DeltaAnalysisException](df.writeTo(tblName).append())
      assert(exV2.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))

    }
  }

  test("streaming") {
    withIdentityColumnTable(GeneratedAlways, tblName) {
      val path = DeltaLog.forTable(spark, TableIdentifier(tblName)).dataPath.toString
      withTempDir { checkpointDir =>
        val ex = intercept[StreamingQueryException] {
          val stream = MemoryStream[Int]
          val q = stream
            .toDF
            .map(_ => Tuple2(1L, 1))
            .toDF("id", "value")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.AvailableNow)
            .start(path)
          stream.addData(1 to 10)
          q.processAllAvailable()
          q.stop()
        }
        assert(ex.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))
      }
    }
  }

  test("update") {
    for (generatedAsIdentityType <- GeneratedAsIdentityType.values) {
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")

        val blockedStatements = Seq(
          // Unconditional UPDATE.
          s"UPDATE $tblName SET id = 1",
          // Conditional UPDATE.
          s"UPDATE $tblName SET id = 1 WHERE value = 2"
        )
        for (stmt <- blockedStatements) {
          val ex = intercept[DeltaAnalysisException](sql(stmt))
          assert(ex.getMessage.contains("UPDATE on IDENTITY column"))
        }
      }
    }
  }
}

class IdentityColumnAdmissionScalaSuite
  extends IdentityColumnAdmissionSuiteBase
    with ScalaDDLTestUtils

