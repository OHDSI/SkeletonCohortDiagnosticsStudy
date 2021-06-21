ROhdsiWebApi::authorizeWebApi(Sys.getenv('baseUrl'), "windows") # Windows authentication - if security enabled using windows authentication


cdmSources <- ROhdsiWebApi::getCdmSources(baseUrl = Sys.getenv('baseUrl')) %>%
  dplyr::mutate(baseUrl = Sys.getenv('baseUrl'),
                dbms = 'redshift',
                sourceDialect = 'redshift',
                port = 5439,
                version = .data$sourceKey %>% substr(., nchar(.) - 3, nchar(.)) %>% as.integer(),
                database = .data$sourceKey %>% substr(., 5, nchar(.) - 6)) %>%
  dplyr::group_by(.data$database) %>%
  dplyr::arrange(dplyr::desc(.data$version)) %>%
  dplyr::mutate(sequence = dplyr::row_number()) %>%
  dplyr::ungroup() %>%
  dplyr::arrange(.data$database, .data$sequence) %>%
  dplyr::mutate(server = tolower(paste0(Sys.getenv("serverRoot"),"/", .data$database))) %>% 
  dplyr::mutate(cohortDatabaseSchema = paste0("scratch_", keyring::key_get(service = keyringUserService))) %>% 
  dplyr::filter(database %in% databaseIds) %>%
  dplyr::filter(sequence == 1)



######
execute <- function(x) {
  if (x$generateCohortTableName) {
    cohortTableName <- paste0(
      stringr::str_squish(x$databaseId),
      stringr::str_replace_all(
        string = Sys.Date(),
        pattern = "-",
        replacement = ""
      ),
      sample(1:10, 1)
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
            )
          if (nrow(cdmDataSource) == 0) {
            return(sourceInfo)
          }
          if (sourceDescription %in% colnames(cdmDataSource)) {
            sourceInfo$sourceDescription <- cdmDataSource$sourceDescription
          }
          if (cdmSourceName %in% colnames(cdmDataSource)) {
            sourceInfo$cdmSourceName <- cdmDataSource$cdmSourceName
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
  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  dataSourceDetails <- getDataSourceDetails(connection = connection,
                                            databaseId = x$databaseId,
                                            cdmDatabaseSchema = cdmDatabaseSchema)
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
    CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)
  }
  
  if (length(x$privateKeyFileName) > 0 && length(x$userName) > 0) {
    CohortDiagnostics::uploadResults(outputFolder,
                                     x$privateKeyFileName,
                                     x$userName)
  }
}