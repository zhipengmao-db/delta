/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.typesafe.tools.mima.core._

/**
 * The list of Mima errors to exclude in the Standalone project.
 */
object StandaloneMimaExcludes {
  val ignoredABIProblems = Seq(
    // scalastyle:off line.size.limit

    // Ignore changes to internal Scala codes
    ProblemFilters.exclude[Problem]("io.delta.standalone.internal.*"),

    // Public API changes in 0.2.0 -> 0.3.0
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.DeltaLog.getChanges"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.DeltaLog.startTransaction"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.Snapshot.scan"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.DeltaLog.tableExists"),

    // Switch to using delta-storage LogStore API in 0.4.0 -> 0.5.0
    ProblemFilters.exclude[MissingClassProblem]("io.delta.standalone.storage.LogStore"),

    // Ignore missing shaded attributes
    ProblemFilters.exclude[Problem]("shadedelta.*"),

    // Public API changes in 0.4.0 -> 0.5.0
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.DeltaLog.getVersionBeforeOrAtTimestamp"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.DeltaLog.getVersionAtOrAfterTimestamp"),

    // ParquetSchemaConverter etc. were moved to project standalone-parquet
    ProblemFilters.exclude[MissingClassProblem]("io.delta.standalone.util.ParquetSchemaConverter"),
    ProblemFilters.exclude[MissingClassProblem]("io.delta.standalone.util.ParquetSchemaConverter$ParquetOutputTimestampType"),

    // Public API changes in 0.5.0 -> 0.6.0
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.OptimisticTransaction.readVersion"),

    // scalastyle:on line.size.limit
  )
}
