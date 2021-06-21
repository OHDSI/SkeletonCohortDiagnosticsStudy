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
  getDataSourceInformation <-
    function(connection,
             cdmDatabaseSchema,
             vocabDatabaseSchema) {
      sqlCdmDataSource <- "select * from @cdmDatabaseSchema.cdm_source;"
      tryCatch(
        expr = {
          cdmDataSource <-
            DatabaseConnector::renderTranslateQuerySql(
              connection = connection,
              sql = sqlCdmDataSource,
              cdmDatabaseSchema = cdmDatabaseSchema,
              snakeCaseToCamelCase = TRUE
            )
          if (sourceDescription %in% colnames(cdmDataSource)) {
            sourceDescription <- cdmDataSource$sourceDescription
          } else {
            sourceDescription <- databaseId
          }
        },
        error = function(...) {
          sourceDescription <- databaseId
        }
      )
      return(sourceDescription)
    }
  
  # Details for connecting to the server:
  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = cdmSource$dbms,
      server = cdmSource$server,
      user = keyring::key_get(service = x$userService),
      password =  keyring::key_get(service = x$passwordService),
      port = cdmSource$port
    )
  # The name of the database schema where the CDM data can be found:
  cdmDatabaseSchema <- cdmSource$cdmDatabaseSchema
  cohortDatabaseSchema <- cdmSource$cohortDatabaseSchema
  
  SkeletonCohortDiagnosticsStudy::execute(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = x$cdmDatabaseSchema,
    cohortDatabaseSchema = x$cohortDatabaseSchema,
    cohortTable = x$cohortTable,
    verifyDependencies = x$verifyDependencies,
    outputFolder = file.path(x$outputFolder, x$databaseId),
    databaseId = x$databaseId,
    databaseName = x$databaseName,
    databaseDescription = x$databaseDescription
  )
  
  if (x$preMergeDiagnosticsFiles) {
    CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(x$outputFolder, x$databaseId))
  }
  
  if (length(x$privateKeyFileName) > 0 && length(x$userName) > 0) {
    CohortDiagnostics::uploadResults(file.path(x$outputFolder, x$databaseId),
                                     x$privateKeyFileName,
                                     x$userName)
  }
}