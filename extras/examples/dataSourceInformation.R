source(Sys.getenv("startUpScriptLocation"))

######
executeOnMultipleDataSources <- function(x) {
  library(magrittr)
  if (x$generateCohortTableName) {
    cohortTableName <- paste0(stringr::str_squish(x$cdmSource$projectCode),
                              stringr::str_squish(x$cdmSource$sourceId))
  }
  
  extraLog <- (
    paste0(
      "Running ",
      x$cdmSource$sourceName,
      " (",
      cdmSource$sourceId,
      ") on ",
      x$cdmSource$runOn,
      "\n     server: ",
      x$cdmSource$runOn,
      " (",
      x$cdmSource$serverFinal,
      ")",
      "\n     cdmDatabaseSchema: ",
      x$cdmSource$cdmDatabaseSchemaFinal,
      "\n     cohortDatabaseSchema: ",
      x$cdmSource$cohortDatabaseSchemaFinal
    )
  )
  
  # Details for connecting to the server:
  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = x$cdmSource$dbms,
      server = x$cdmSource$serverFinal,
      user = keyring::key_get(service = x$userService),
      password =  keyring::key_get(service = x$passwordService),
      port = x$cdmSource$port
    )
  # The name of the database schema where the CDM data can be found:
  cdmDatabaseSchema <- x$cdmSource$cdmDatabaseSchemaFinal
  vocabDatabaseSchema <- x$cdmSource$vocabDatabaseSchemaFinal
  cohortDatabaseSchema <- x$cdmSource$cohortDatabaseSchemaFinal
  
  databaseId <- x$databaseId
  databaseName <- x$databaseName
  databaseDescription <- x$databaseDescription
  
  SkeletonCohortDiagnosticsStudy::execute(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName,
    verifyDependencies = x$verifyDependencies,
    outputFolder = x$outputFolder,
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    extraLog = extraLog
  )
}
