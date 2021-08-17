source(Sys.getenv("startUpScriptLocation"))

######
execute <- function(x) {
  if (x$generateCohortTableName) {
    cohortTableName <- paste0(
      stringr::str_squish(x$databaseId),
      stringr::str_squish("SkeletonCohortDiagnosticsStudy")
    )
  }
  
  # this function gets details of the data source from cdm source table in omop, if populated.
  # The assumption is the cdm_source.sourceDescription has text description of data source.
  getDataSourceDetails <-
    function(connection,
             databaseId,
             cdmDatabaseSchema) {
      sqlCdmDataSource <- "select * from @cdmDatabaseSchema.cdm_source;"
      sourceInfo <- list(cdmSourceName = databaseId,
                         sourceDescription = databaseId)
      tryCatch(
        expr = {
          cdmDataSource <-
            DatabaseConnector::renderTranslateQuerySql(
              connection = connection,
              sql = sqlCdmDataSource,
              cdmDatabaseSchema = cdmDatabaseSchema,
              snakeCaseToCamelCase = TRUE
            ) %>% dplyr::tibble()
          if (nrow(cdmDataSource) == 0) {
            return(sourceInfo)
          }
          sourceInfo$sourceDescription <- ""
          if ("sourceDescription" %in% colnames(cdmDataSource)) {
            sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                   " ",
                                                   cdmDataSource$sourceDescription) %>% 
              stringr::str_trim()
          }
          if ("cdmSourceName" %in% colnames(cdmDataSource)) {
            sourceInfo$cdmSourceName <- cdmDataSource$cdmSourceName
          }
          if ("cdmEtlReference" %in% colnames(cdmDataSource)) {
            if (length(cdmDataSource$cdmEtlReference) > 4) {
              sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                     " ETL Reference: ",
                                                     cdmDataSource$cdmEtlReference) %>% 
                stringr::str_trim()
            } else {
              sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                     " ETL Reference: None") %>% 
                stringr::str_trim()
            }
          }
          if ("sourceReleaseDate" %in% colnames(cdmDataSource)) {
            sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                   " CDM release date: ",
                                                   as.character(cdmDataSource$sourceReleaseDate)) %>% 
              stringr::str_trim()
          } else {
            sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                   " CDM release date: None") %>% 
              stringr::str_trim()
          }
          if ("sourceReleaseDate" %in% colnames(cdmDataSource)) {
            sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                   " Source release date: ",
                                                   as.character(cdmDataSource$sourceReleaseDate)) %>% 
              stringr::str_trim()
          } else {
            sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                   " Source release date: None") %>% 
              stringr::str_trim()
          }
          if ("sourceDocumentationReference" %in% colnames(cdmDataSource)) {
            sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                   " Source Documentation Reference: ",
                                                   as.character(cdmDataSource$sourceDocumentationReference)) %>% 
              stringr::str_trim()
          } else {
            sourceInfo$sourceDescription <- paste0(sourceInfo$sourceDescription,
                                                   " Source Documentation Reference: None") %>% 
              stringr::str_trim()
          }
        },
        error = function(...) {
          return(sourceInfo)
        }
      )
      return(sourceInfo)
    }
  
  # Details for connecting to the server:
  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = x$cdmSource$dbms,
      server = x$cdmSource$server,
      user = keyring::key_get(service = x$userService),
      password =  keyring::key_get(service = x$passwordService),
      port = x$cdmSource$port
    )
  # The name of the database schema where the CDM data can be found:
  cdmDatabaseSchema <- x$cdmSource$cdmDatabaseSchema
  cohortDatabaseSchema <- x$cdmSource$cohortDatabaseSchema
  
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  dataSourceDetails <- getDataSourceDetails(
    connection = connection,
    databaseId = x$databaseId,
    cdmDatabaseSchema = cdmDatabaseSchema
  )
  DatabaseConnector::disconnect(connection)
  
  SkeletonCohortDiagnosticsStudy::execute(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName,
    verifyDependencies = x$verifyDependencies,
    outputFolder = x$outputFolder,
    databaseId = x$databaseId,
    databaseName = dataSourceDetails$databaseName,
    databaseDescription = dataSourceDetails$databaseDescription
  )
  
  if (x$preMergeDiagnosticsFiles) {
    CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = x$outputFolder)
  }
  
  if (length(x$privateKeyFileName) > 0 &&
      !x$privateKeyFileName == "" &&
      length(x$userName) > 0 &&
      !x$userName == "") {
    CohortDiagnostics::uploadResults(x$outputFolder,
                                     x$privateKeyFileName,
                                     x$userName)
  }
  
  if (length(x$uploadToLocalPostGresDatabaseSpecifications) > 1) {
    # Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable bulk upload (recommended).
    
    connectionPostGres <-
      DatabaseConnector::connect(x$uploadToLocalPostGresDatabaseSpecifications$connectionDetails)
    
    # check if schema was instantiated
    sqlSchemaCheck <-
      paste0(
        "SELECT * FROM information_schema.schemata WHERE schema_name = "",
        tolower(x$uploadToLocalPostGresDatabaseSpecifications$schema),
        "";"
      )
    schemaExists <-
      DatabaseConnector::renderTranslateQuerySql(connection = connectionPostGres,
                                                 sql = sqlSchemaCheck)
    
    if (nrow(schemaExists) == 0) {
      warning(
        paste0(
          "While attempting to upload to postgres, found target schema to not exist - attempting to create target schema ",
          x$uploadToLocalPostGresDatabaseSpecifications$schema
        )
      )
      createSchemaSql <-
        paste0(
          "select create_schema('",
          tolower(x$uploadToLocalPostGresDatabaseSpecifications$schema),
          ");"
        )
      DatabaseConnector::renderTranslateQuerySql(connection = connectionPostGres,
                                                 sql = createSchemaSql)
      ParallelLogger::logInfo("Schema created.")
      
    }
    # check if required table exists, else create them
    if (!DatabaseConnector::dbExistsTable(conn = connectionPostGres, name = "cohort_count")) {
      CohortDiagnostics::createResultsDataModel(
        connection = connectionPostGres,
        schema = tolower(x$uploadToLocalPostGresDatabaseSpecifications$schema)
      )
    }
    
    DatabaseConnector::disconnect(connection = connectionPostGres)
    # note this is a thread safe upload, so its ok to parallelize
    CohortDiagnostics::uploadResults(
      connectionDetails = x$uploadToLocalPostGresDatabaseSpecifications$connectionDetails,
      schema = x$uploadToLocalPostGresDatabaseSpecifications$schema,
      zipFileName = x$uploadToLocalPostGresDatabaseSpecifications$zipFileName
    )
  }
}