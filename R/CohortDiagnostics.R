# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of SkeletonCohortDiagnosticsStudy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Execute the cohort diagnostics
#'
#' @details
#' This function executes the cohort diagnostics.
#'
#' @param connectionDetails                   An object of type \code{connectionDetails} as created
#'                                            using the
#'                                            \code{\link[DatabaseConnector]{createConnectionDetails}}
#'                                            function in the DatabaseConnector package.
#' @param cdmDatabaseSchema                   Schema name where your patient-level data in OMOP CDM
#'                                            format resides. Note that for SQL Server, this should
#'                                            include both the database and schema name, for example
#'                                            'cdm_data.dbo'.
#' @param cohortDatabaseSchema                Schema name where intermediate data can be stored. You
#'                                            will need to have write privileges in this schema. Note
#'                                            that for SQL Server, this should include both the
#'                                            database and schema name, for example 'cdm_data.dbo'.
#' @param vocabularyDatabaseSchema            Schema name where your OMOP vocabulary data resides. This
#'                                            is commonly the same as cdmDatabaseSchema. Note that for
#'                                            SQL Server, this should include both the database and
#'                                            schema name, for example 'vocabulary.dbo'.
#' @param cohortTable                         The name of the table that will be created in the work
#'                                            database schema. This table will hold the exposure and
#'                                            outcome cohorts used in this study.
#' @param tempEmulationSchema                 Some database platforms like Oracle and Impala do not
#'                                            truly support temp tables. To emulate temp tables,
#'                                            provide a schema with write privileges where temp tables
#'                                            can be created.
#' @param verifyDependencies                  Check whether correct package versions are installed?
#' @param outputFolder                        Name of local folder to place results; make sure to use
#'                                            forward slashes (/). Do not use a folder on a network
#'                                            drive since this greatly impacts performance.
#' @param databaseId                          A short string for identifying the database (e.g.
#'                                            'Synpuf').
#' @param databaseName                        The full name of the database (e.g. 'Medicare Claims
#'                                            Synthetic Public Use Files (SynPUFs)').
#' @param databaseDescription                 A short description (several sentences) of the database.
#' @param incrementalFolder                   Name of local folder to hold the logs for incremental
#'                                            run; make sure to use forward slashes (/). Do not use a
#'                                            folder on a network drive since this greatly impacts
#'                                            performance.
#'
#' @export
execute <- function(connectionDetails,
                    cdmDatabaseSchema,
                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                    cohortDatabaseSchema = cdmDatabaseSchema,
                    cohortTable = "cohort",
                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                    verifyDependencies = TRUE,
                    outputFolder,
                    incrementalFolder = file.path(outputFolder, "incrementalFolder"),
                    databaseId = "Unknown",
                    databaseName = databaseId,
                    databaseDescription = databaseId) {
  if (!file.exists(outputFolder))
    dir.create(outputFolder, recursive = TRUE)
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(
    ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
    add = TRUE
  )
  
  if (verifyDependencies) {
    ParallelLogger::logInfo("Checking whether correct package versions are installed")
    verifyDependencies()
  }
  
  ParallelLogger::logInfo("Creating cohorts")
  
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = TRUE
  )
  
  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = TRUE
  )
  
  CohortGenerator::exportCohortStatsTables(
    connectionDetails = connectionDetails,
    connection = NULL,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = outputFolder,
    incremental = TRUE
  )
  
  cohortDefinitionSet <-
    loadCohortsFromPackage(packageName = "SkeletonCohortDiagnosticsStudy",
                           cohortToCreateFile = "settings/CohortsToCreateForTesting.csv")
  
  
  executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = outputFolder,
    databaseId = databaseId,
    connectionDetails = connectionDetails,
    connection = NULL,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortTableNames = cohortTableNames,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortIds = NULL,
    inclusionStatisticsFolder = outputFolder,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    cdmVersion = 5,
    runInclusionStatistics = TRUE,
    runIncludedSourceConcepts = TRUE,
    runOrphanConcepts = TRUE,
    runTimeDistributions = TRUE,
    runVisitContext = TRUE,
    runBreakdownIndexEvents = TRUE,
    runIncidenceRate = TRUE,
    runTimeSeries = FALSE,
    runCohortOverlap = TRUE,
    runCohortCharacterization = TRUE,
    covariateSettings = createDefaultCovariateSettings(),
    runTemporalCohortCharacterization = TRUE,
    temporalCovariateSettings = createTemporalCovariateSettings(
      useConditionOccurrence =
        TRUE,
      useDrugEraStart = TRUE,
      useProcedureOccurrence = TRUE,
      useMeasurement = TRUE,
      temporalStartDays = c(-365,-30, 0, 1, 31),
      temporalEndDays = c(-31,-1, 0, 30, 365)
    ),
    minCellCount = 5,
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )
  
  CohortGenerator::dropCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    connection = NULL
  )
}
