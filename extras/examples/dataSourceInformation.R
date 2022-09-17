source(Sys.getenv("startUpScriptLocation"))

######
executeOnMultipleDataSources <- function(x) {
  library(magrittr)
  if (x$generateCohortTableName) {
    cohortTableName <- paste0(stringr::str_squish(x$projectCode),
                              stringr::str_squish(x$sourceId))
  }
  
  extraLog <- (
    paste0(
      "Running ",
      x$databaseName,
      " (",
      x$sourceId,
      ") on ",
      x$sourceId,
      "\n     server: ",
      x$runOn,
      " (",
      x$serverFinal,
      ")",
      "\n     cdmDatabaseSchema: ",
      x$cdmDatabaseSchema,
      "\n     cohortDatabaseSchema: ",
      x$cohortDatabaseSchema
    )
  )
  
  # Details for connecting to the server:
  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = x$dbms,
      server = x$server,
      user = keyring::key_get(service = x$userService),
      password =  keyring::key_get(service = x$passwordService),
      port = x$port
    )
  # The name of the database schema where the CDM data can be found:
  cdmDatabaseSchema <- x$cdmDatabaseSchema
  vocabDatabaseSchema <- x$vocabDatabaseSchema
  cohortDatabaseSchema <- x$cohortDatabaseSchema
  
  databaseId <- x$databaseId
  
  SkeletonCohortDiagnosticsStudy::execute(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName,
    verifyDependencies = x$verifyDependencies,
    outputFolder = x$outputFolder,
    databaseId = databaseId,
    extraLog = extraLog
  )
}
