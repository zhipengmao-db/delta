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

import scala.collection.mutable.ListBuffer

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedAsIdentityType, GeneratedByDefault}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * General test suite for identity columns.
 */
trait IdentityColumnSuiteBase extends IdentityColumnTestUtils {

  import testImplicits._
  protected val tblName = "identity_test"

  // For OSS only, because we have legacy SQL ParseException for DBR when this flag is off. We don't
  // need this test in DBR, since we already have tests for the other exception.
  test("Don't allow IDENTITY column in the schema if the feature is disabled") {
    withSQLConf(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key -> "false") {
      withTable(tblName) {
        val e = intercept[DeltaUnsupportedTableFeatureException] {
          createTableWithIdColAndIntValueCol(
            tblName, GeneratedByDefault, startsWith = None, incrementBy = None)
        }
        val errorMsg = e.getMessage
        assert(errorMsg.contains("requires writer table feature(s) that are unsupported"))
        assert(errorMsg.contains(IdentityColumnsTableFeature.name))
      }
    }
  }

  // Build expected schema of the following table definition for verification:
  // CREATE TABLE tableName (
  //   id BIGINT <keyword> IDENTITY (START WITH <start> INCREMENT BY <step>),
  //   value INT
  // );
  private def expectedSchema(
      generatedAsIdentityType: GeneratedAsIdentityType,
      start: Long = IdentityColumn.defaultStart,
      step: Long = IdentityColumn.defaultStep): StructType = {
    val colFields = new ListBuffer[StructField]

    val allowExplicitInsert = generatedAsIdentityType == GeneratedByDefault
    val builder = new MetadataBuilder()
    builder.putBoolean(DeltaSourceUtils.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT,
      allowExplicitInsert)
    builder.putLong(DeltaSourceUtils.IDENTITY_INFO_START, start)
    builder.putLong(DeltaSourceUtils.IDENTITY_INFO_STEP, step)
    colFields += StructField("id", LongType, true, builder.build())
    colFields += StructField("value", IntegerType)

    StructType(colFields.toSeq)
  }

  test("various configuration") {
    val starts = Seq(
      Long.MinValue,
      Integer.MIN_VALUE.toLong,
      -100L,
      0L,
      1000L,
      Integer.MAX_VALUE.toLong,
      Long.MaxValue
    )
    val steps = Seq(
      Long.MinValue,
      Integer.MIN_VALUE.toLong,
      -100L,
      1000L,
      Integer.MAX_VALUE.toLong,
      Long.MaxValue
    )
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      startsWith <- starts
      incrementBy <- steps
    } {
      withTable(tblName) {
        createTableWithIdColAndIntValueCol(
          tblName, generatedAsIdentityType, Some(startsWith), Some(incrementBy))
        val table = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val actualSchema =
          DeltaColumnMapping.dropColumnMappingMetadata(table.snapshot.metadata.schema)
        assert(actualSchema === expectedSchema(generatedAsIdentityType, startsWith, incrementBy))
      }
    }
  }

  test("default configuration") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      startsWith <- Seq(Some(1L), None)
      incrementBy <- Seq(Some(1L), None)
    } {
      withTable(tblName) {
        createTableWithIdColAndIntValueCol(
          tblName, generatedAsIdentityType, startsWith, incrementBy)
        val table = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val actualSchema =
          DeltaColumnMapping.dropColumnMappingMetadata(table.snapshot.metadata.schema)
        assert(actualSchema === expectedSchema(generatedAsIdentityType))
      }
    }
  }

}

class IdentityColumnScalaSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils {

  test("unsupported column type") {
    val tblName = "identity_test"
    for (unsupportedType <- unsupportedDataTypes) {
      withTable(tblName) {
        val ex = intercept[DeltaUnsupportedOperationException] {
          createTable(
            tblName,
            Seq(
              IdentityColumnSpec(GeneratedAlways, dataType = unsupportedType),
              TestColumnSpec(colName = "value", dataType = StringType)
            )
          )
        }
        assert(ex.getErrorClass === "DELTA_IDENTITY_COLUMNS_UNSUPPORTED_DATA_TYPE")
        assert(ex.getMessage.contains("is not supported for IDENTITY columns"))
      }
    }
  }

  test("unsupported step") {
    val tblName = "identity_test"
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      startsWith <- Seq(Some(1L), None)
    } {
      withTable(tblName) {
        val ex = intercept[DeltaAnalysisException] {
          createTableWithIdColAndIntValueCol(
            tblName, generatedAsIdentityType, startsWith, incrementBy = Some(0))
        }
        assert(ex.getErrorClass === "DELTA_IDENTITY_COLUMNS_ILLEGAL_STEP")
        assert(ex.getMessage.contains("step cannot be 0."))
      }
    }
  }

  test("cannot specify generatedAlwaysAs with identity columns") {
    def expectColumnBuilderError(f: => StructField): Unit = {
      val ex = intercept[DeltaAnalysisException] {
        f
      }
      assert(ex.getErrorClass === "DELTA_IDENTITY_COLUMNS_WITH_GENERATED_EXPRESSION")
      ex.getMessage.contains(
        "Identity column cannot be specified with a generated column expression.")
    }
    val generatedColumn = io.delta.tables.DeltaTable.columnBuilder(spark, "id")
      .dataType(LongType)
      .generatedAlwaysAs("id + 1")

    expectColumnBuilderError {
      generatedColumn.generatedAlwaysAsIdentity().build()
    }

    expectColumnBuilderError {
      generatedColumn.generatedByDefaultAsIdentity().build()
    }
  }
}

class IdentityColumnScalaIdColumnMappingSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableIdMode

class IdentityColumnScalaNameColumnMappingSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableNameMode
