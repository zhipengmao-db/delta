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

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedAsIdentityType}
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.apache.spark.util.Utils

// Test command that should be allowed and disallowed on IDENTITY columns.
class IdentityColumnAdmissionSuite
  extends IdentityColumnTestUtils with ScalaDDLTestUtils {

  import testImplicits._

  private val tblName = "identity_admission_test"

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      // For schema migration to work.
      .set(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key, "true")
  }

  test("alter table add column") {
    for (generatedAsIdentityType <- GeneratedAsIdentityType.values) {
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        val testColumnSpec = new IdentityColumnSpec(generatedAsIdentityType, colName = "id2")
        intercept[ParseException]{
          sql(s"ALTER TABLE $tblName ADD COLUMNS (${testColumnSpec.ddl})")
        }
      }
    }
  }

  test("alter table change column type") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      keyword <- Seq("ALTER", "CHANGE")
      targetType <- Seq(IntegerType, DoubleType)
    } {
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        val ex = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tblName $keyword COLUMN id TYPE ${targetType.sql}")
        }
        assert(ex.getMessage.contains("ALTER TABLE ALTER COLUMN is not supported"))
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
        val ex = intercept[AnalysisException] {
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
        val ex1 = intercept[AnalysisException] {
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
        val ex2 = intercept[AnalysisException] {
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
        val ex1 = intercept[AnalysisException] {
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
        val ex2 = intercept[AnalysisException] {
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
        val ex = intercept[AnalysisException](sql(stmt))
        assert(ex.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))
      }

      // Test DataFrame V1 and V2 API.
      val df = (1 to 10).map(v => (v.toLong, v)).toDF("id", "value")

      val path = DeltaLog.forTable(spark, TableIdentifier(tblName)).dataPath.toString
      val exV1 = intercept[AnalysisException](df.write.format("delta").mode("append").save(path))
      assert(exV1.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))

      val exV2 = intercept[AnalysisException](df.writeTo(tblName).append())
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

  test("delete") {
    for (generatedAsIdentityType <- GeneratedAsIdentityType.values) {
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")
        val prevMax = sql(s"SELECT MAX(id) FROM $tblName").collect().head.getLong(0)
        sql(s"DELETE FROM $tblName WHERE value = 1")
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName"),
          Row(1L)
        )
        sql(s"DELETE FROM $tblName")
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName"),
          Row(0L)
        )
        sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName where id <= $prevMax"),
          Row(0L)
        )
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
          val ex = intercept[AnalysisException](sql(stmt))
          assert(ex.getMessage.contains("UPDATE on IDENTITY column"))
        }
      }
    }
  }

  test("merge") {
    val source = "identity_admission_source"
    val target = "identity_admission_tagert"
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
    } {
      withIdentityColumnTable(generatedAsIdentityType, target) {
        withTable(source) {
          sql(
            s"""
               |CREATE TABLE $source (
               |  value INT,
               |  id BIGINT
               |) USING delta
               |""".stripMargin)
          sql(
            s"""
               |INSERT INTO $source VALUES (1, 100), (2, 200), (3, 300)
               |""".stripMargin)
          sql(
            s"""
               |INSERT INTO $target(value) VALUES (2), (3), (4)
               |""".stripMargin)

          val updateStmt =
            s"""
               |MERGE INTO $target
               |  USING $source on $target.value = $source.value
               |  WHEN MATCHED THEN UPDATE SET *
               |""".stripMargin
          val updateEx = intercept[AnalysisException](sql(updateStmt))
          assert(updateEx.getMessage.contains("UPDATE on IDENTITY column"))

          val insertStmt =
            s"""
               |MERGE INTO $target
               |  USING $source on $target.value = $source.value
               |  WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin

          if (generatedAsIdentityType == GeneratedAlways) {
            val insertEx = intercept[AnalysisException](sql(insertStmt))
            assert(
              insertEx.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))
          } else {
            sql(insertStmt)
          }
        }
      }
    }
  }

  test("clone") {
      val oldTbl = "identity_admission_old"
      val newTbl = "identity_admission_new"
      for {
        generatedAsIdentityType <- GeneratedAsIdentityType.values
        cloneType <- Seq("SHALLOW")
      } {
        withIdentityColumnTable(generatedAsIdentityType, oldTbl) {
          withTable(newTbl) {
            sql(s"INSERT INTO $oldTbl (value) VALUES (1), (2)")
            val oldSchema = DeltaLog.forTable(spark, TableIdentifier(oldTbl)).snapshot.schema

            sql(
              s"""
                 |CREATE TABLE $newTbl
                 |  $cloneType CLONE $oldTbl
                 |""".stripMargin)
            val newSchema = DeltaLog.forTable(spark, TableIdentifier(newTbl)).snapshot.schema

            assert(newSchema("id").metadata.getLong(DeltaSourceUtils.IDENTITY_INFO_START) == 1L)
            assert(newSchema("id").metadata.getLong(DeltaSourceUtils.IDENTITY_INFO_STEP) == 1L)
            assert(oldSchema == newSchema)

            sql(s"INSERT INTO $newTbl (value) VALUES (1), (2)")
            checkAnswer(
              sql(s"SELECT COUNT(DISTINCT id) FROM $newTbl"),
              Row(4L)
            )
          }
        }
      }
  }

  test("restore") {
      for (generatedAsIdentityType <- GeneratedAsIdentityType.values) {
        withTable(tblName) {
          // v0.
          createTable(
            tblName,
            Seq(
              IdentityColumnSpec(generatedAsIdentityType),
              TestColumnSpec(colName = "value", dataType = IntegerType)
            ),
            partitionedBy = Seq("value")
          )
          // v1.
          sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")
          val v1Content = sql(s"SELECT * FROM $tblName").collect()
          // v2.
          sql(s"INSERT INTO $tblName (value) VALUES (3), (4)")
          // v3: RESTORE to v1.
          sql(s"RESTORE TABLE $tblName TO VERSION AS OF 1")
          checkAnswer(
            sql(s"SELECT COUNT(DISTINCT id) FROM $tblName"),
            Row(2L)
          )
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            v1Content
          )
          // v4.
          sql(s"INSERT INTO $tblName (value) VALUES (5), (6)")
          checkAnswer(
            sql(s"SELECT COUNT(DISTINCT id) FROM $tblName"),
            Row(4L)
          )
        }
      }
    }
}
