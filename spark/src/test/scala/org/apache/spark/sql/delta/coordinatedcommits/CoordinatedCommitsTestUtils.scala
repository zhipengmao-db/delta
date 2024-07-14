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

package org.apache.spark.sql.delta.coordinatedcommits

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaTestUtilsBase}
import org.apache.spark.sql.delta.DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

trait CoordinatedCommitsTestUtils
  extends DeltaTestUtilsBase { self: SparkFunSuite with SharedSparkSession =>

  /**
   * Runs a specific test with coordinated commits default properties unset.
   * Any table created in this test won't have coordinated commits enabled by default.
   */
  def testWithDefaultCommitCoordinatorUnset(testName: String)(f: => Unit): Unit = {
    test(testName) {
      withoutCoordinatedCommitsDefaultTableProperties {
        f
      }
    }
  }

  /**
   * Runs the function `f` with coordinated commits default properties unset.
   * Any table created in function `f`` won't have coordinated commits enabled by default.
   */
  def withoutCoordinatedCommitsDefaultTableProperties[T](f: => T): T = {
    val commitCoordinatorKey = COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey
    val oldCommitCoordinatorValue = spark.conf.getOption(commitCoordinatorKey)
    spark.conf.unset(commitCoordinatorKey)
    try { f } finally {
      oldCommitCoordinatorValue.foreach {
        spark.conf.set(commitCoordinatorKey, _)
      }
    }
  }

  /**
   * Runs the function `f` with coordinated commits default properties set to what is specified.
   * Any table created in function `f` will have the `commitCoordinator` property set to the
   * specified `commitCoordinatorName`.
   */
  def withCustomCoordinatedCommitsTableProperties(
      commitCoordinatorName: String,
      conf: Map[String, String] = Map("randomConf" -> "randomConfValue"))(f: => Unit): Unit = {
    withSQLConf(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
        commitCoordinatorName,
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey ->
        JsonUtils.toJson(conf)) {
      f
    }
  }

  /** Run the test with different backfill batch sizes: 1, 2, 10 */
  def testWithDifferentBackfillInterval(testName: String)(f: Int => Unit): Unit = {
    Seq(1, 2, 10).foreach { backfillBatchSize =>
      test(s"$testName [Backfill batch size: $backfillBatchSize]") {
        CommitCoordinatorProvider.clearNonDefaultBuilders()
        CommitCoordinatorProvider.registerBuilder(
          TrackingInMemoryCommitCoordinatorBuilder(backfillBatchSize))
        CommitCoordinatorProvider.registerBuilder(
          InMemoryCommitCoordinatorBuilder(backfillBatchSize))
        f(backfillBatchSize)
      }
    }
  }

  /**
   * Run the test against a [[TrackingCommitCoordinatorClient]] with backfill batch size =
   * `batchBackfillSize`
   */
  def testWithCoordinatedCommits(backfillBatchSize: Int)(testName: String)(f: => Unit): Unit = {
    test(s"$testName [Backfill batch size: $backfillBatchSize]") {
      CommitCoordinatorProvider.clearNonDefaultBuilders()
      CommitCoordinatorProvider.registerBuilder(
        TrackingInMemoryCommitCoordinatorBuilder(backfillBatchSize))
      val coordinatedCommitsCoordinatorConf = Map("randomConf" -> "randomConfValue")
      val coordinatedCommitsCoordinatorJson = JsonUtils.toJson(coordinatedCommitsCoordinatorConf)
      withSQLConf(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
            "tracking-in-memory",
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey ->
            coordinatedCommitsCoordinatorJson) {
        f
      }
    }
  }

  /** Run the test with:
   * 1. Without coordinated-commits
   * 2. With coordinated-commits with different backfill batch sizes
   */
  def testWithDifferentBackfillIntervalOptional(testName: String)(f: Option[Int] => Unit): Unit = {
    test(s"$testName [Backfill batch size: None]") {
      f(None)
    }
    testWithDifferentBackfillInterval(testName) { backfillBatchSize =>
      val coordinatedCommitsCoordinatorConf = Map("randomConf" -> "randomConfValue")
      val coordinatedCommitsCoordinatorJson = JsonUtils.toJson(coordinatedCommitsCoordinatorConf)
      withSQLConf(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
            "tracking-in-memory",
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey ->
            coordinatedCommitsCoordinatorJson) {
        f(Some(backfillBatchSize))
      }
    }
  }

  def getUpdatedActionsForZerothCommit(
      commitInfo: CommitInfo,
      oldMetadata: Metadata = Metadata()): UpdatedActions = {
    val newMetadataConfiguration =
      oldMetadata.configuration +
        (DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> "tracking-in-memory")
    val newMetadata = oldMetadata.copy(configuration = newMetadataConfiguration)
    UpdatedActions(commitInfo, newMetadata, Protocol(), oldMetadata, Protocol())
  }

  def getUpdatedActionsForNonZerothCommit(commitInfo: CommitInfo): UpdatedActions = {
    val updatedActions = getUpdatedActionsForZerothCommit(commitInfo)
    updatedActions.copy(oldMetadata = updatedActions.getNewMetadata)
  }
}

case class TrackingInMemoryCommitCoordinatorBuilder(
    batchSize: Long,
    defaultCommitCoordinatorClientOpt: Option[CommitCoordinatorClient] = None)
  extends CommitCoordinatorBuilder {
  lazy val trackingInMemoryCommitCoordinatorClient =
    defaultCommitCoordinatorClientOpt.getOrElse {
      new TrackingCommitCoordinatorClient(
        new PredictableUuidInMemoryCommitCoordinatorClient(batchSize))
    }

  override def getName: String = "tracking-in-memory"
  override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
    trackingInMemoryCommitCoordinatorClient
  }
}

class PredictableUuidInMemoryCommitCoordinatorClient(batchSize: Long)
  extends InMemoryCommitCoordinator(batchSize) {

  var nextUuidSuffix = 1L
  override def generateUUID(): String = {
    nextUuidSuffix += 1
    s"uuid-${nextUuidSuffix - 1}"
  }
}

object TrackingCommitCoordinatorClient {
  private val insideOperation = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
}

class TrackingCommitCoordinatorClient(
    val delegatingCommitCoordinatorClient: InMemoryCommitCoordinator)
  extends CommitCoordinatorClient {

  val numCommitsCalled = new AtomicInteger(0)
  val numGetCommitsCalled = new AtomicInteger(0)
  val numBackfillToVersionCalled = new AtomicInteger(0)
  val numRegisterTableCalled = new AtomicInteger(0)

  def recordOperation[T](op: String)(f: => T): T = {
    val oldInsideOperation = TrackingCommitCoordinatorClient.insideOperation.get()
    try {
      if (!TrackingCommitCoordinatorClient.insideOperation.get()) {
        op match {
          case "commit" => numCommitsCalled.incrementAndGet()
          case "getCommits" => numGetCommitsCalled.incrementAndGet()
          case "backfillToVersion" => numBackfillToVersionCalled.incrementAndGet()
          case "registerTable" => numRegisterTableCalled.incrementAndGet()
          case _ => ()
        }
      }
      TrackingCommitCoordinatorClient.insideOperation.set(true)
      f
    } finally {
      TrackingCommitCoordinatorClient.insideOperation.set(oldInsideOperation)
    }
  }

  override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = recordOperation("commit") {
    delegatingCommitCoordinatorClient.commit(
      logStore,
      hadoopConf,
      logPath,
      coordinatedCommitsTableConf,
      commitVersion,
      actions,
      updatedActions)
  }

  override def getCommits(
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      startVersion: Option[Long],
      endVersion: Option[Long] = None): GetCommitsResponse = recordOperation("getCommits") {
    delegatingCommitCoordinatorClient.getCommits(
      logPath, coordinatedCommitsTableConf, startVersion, endVersion)
  }

  def removeCommitTestOnly(
      logPath: Path,
      commitVersion: Long
  ): Unit = {
    val tableData = delegatingCommitCoordinatorClient.perTableMap.get(logPath)
    tableData.commitsMap.remove(commitVersion)
    if (commitVersion == tableData.maxCommitVersion) {
      tableData.maxCommitVersion -= 1
    }
  }

  override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      version: Long,
      lastKnownBackfilledVersion: Option[Long]): Unit = recordOperation("backfillToVersion") {
    delegatingCommitCoordinatorClient.backfillToVersion(
      logStore,
      hadoopConf,
      logPath,
      coordinatedCommitsTableConf,
      version,
      lastKnownBackfilledVersion)
  }

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = this == other

  def reset(): Unit = {
    numCommitsCalled.set(0)
    numGetCommitsCalled.set(0)
    numBackfillToVersionCalled.set(0)
  }

  override def registerTable(
      logPath: Path,
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): Map[String, String] = recordOperation("registerTable") {
    delegatingCommitCoordinatorClient.registerTable(
      logPath, currentVersion, currentMetadata, currentProtocol)
  }
}

/**
 * A helper class which enables coordinated-commits for the test suite based on the given
 * `coordinatedCommitsBackfillBatchSize` conf.
 */
trait CoordinatedCommitsBaseSuite extends SparkFunSuite with SharedSparkSession {

  // If this config is not overridden, coordinated commits are disabled.
  def coordinatedCommitsBackfillBatchSize: Option[Int] = None

  final def coordinatedCommitsEnabledInTests: Boolean = coordinatedCommitsBackfillBatchSize.nonEmpty

  override protected def sparkConf: SparkConf = {
    if (coordinatedCommitsBackfillBatchSize.nonEmpty) {
      val coordinatedCommitsCoordinatorConf = Map("randomConf" -> "randomConfValue")
      val coordinatedCommitsCoordinatorJson = JsonUtils.toJson(coordinatedCommitsCoordinatorConf)
      super.sparkConf
        .set(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey,
          "tracking-in-memory")
        .set(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey,
          coordinatedCommitsCoordinatorJson)
    } else {
      super.sparkConf
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    coordinatedCommitsBackfillBatchSize.foreach { batchSize =>
      CommitCoordinatorProvider.registerBuilder(TrackingInMemoryCommitCoordinatorBuilder(batchSize))
    }
    DeltaLog.clearCache()
  }

  protected def isICTEnabledForNewTables: Boolean = {
    spark.conf.getOption(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey).nonEmpty ||
      spark.conf.getOption(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey).contains("true")
  }
}
