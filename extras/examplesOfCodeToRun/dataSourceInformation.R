source(Sys.getenv("startUpScriptLocation"))

######
executeOnMultipleDataSources <- function(x) {
  if (x$generateCohortTableName) {
    cohortTableName <- paste0(
      stringr::str_squish(x$databaseId),
      stringr::str_squish("SkeletonCohortDiagnosticsStudy")
    )
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
  vocabDatabaseSchema <- x$cdmSource$vocabDatabaseSchema
  cohortDatabaseSchema <- x$cdmSource$cohortDatabaseSchema

  dataSourceDetails <- getDataSourceInformation(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabDatabaseSchema = vocabDatabaseSchema
  )
  
  SkeletonCohortDiagnosticsStudy::execute(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableName,
    verifyDependencies = x$verifyDependencies,
    outputFolder = x$outputFolder,
    databaseId = x$databaseId,
    databaseName = dataSourceDetails$cdmSourceName,
    databaseDescription = dataSourceDetails$sourceDescription
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
        "SELECT * FROM information_schema.schemata WHERE schema_name = '",
        tolower(x$uploadToLocalPostGresDatabaseSpecifications$schema),
        "';"
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



# this function gets details of the data source from cdm source table in omop, if populated.
# The assumption is the cdm_source.sourceDescription has text description of data source.

getDataSourceInformation <-
  function(connectionDetails,
           cdmDatabaseSchema,
           vocabDatabaseSchema) {
    sqlCdmDataSource <- "select * from @cdmDatabaseSchema.cdm_source;"
    sqlVocabularyVersion <-
      "select * from @vocabDatabaseSchema.vocabulary where vocabulary_id = 'None';"
    etlVersionNumber <- "select * from @cdmDatabaseSchema._version;"
    sourceInfo <- list(cdmSourceName = databaseId,
                       sourceDescription = databaseId)
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    } else {
      return(NULL)
    }
    
    vocabulary <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sqlVocabularyVersion,
        vocabDatabaseSchema = vocabDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble() %>%
      dplyr::rename(vocabularyVersion = .data$vocabularyVersion)
    
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
        } else {
          cdmDataSource <- cdmDataSource %>%
            dplyr::tibble() %>%
            dplyr::rename(vocabularyVersionCdm = .data$vocabularyVersion)
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
    
    version <- dplyr::tibble()
    tryCatch(
      expr = {
        version <-
          DatabaseConnector::renderTranslateQuerySql(
            connection = connection,
            sql = etlVersionNumber,
            cdmDatabaseSchema = cdmDatabaseSchema,
            snakeCaseToCamelCase = TRUE
          ) %>%
          dplyr::tibble() %>% 
          dplyr::mutate(rn = dplyr::row_number()) %>% 
          dplyr::filter(.data$rn == 1) %>% 
          dplyr::select(-.data$rn)
      },
      error = function(...) {
        return(NULL)
      }
    )
    
    if (nrow(version) == 1) {
      if (all('versionId' %in% colnames(version),
              'versionDate' %in% colnames(version))) {
        cdmDataSource <- cdmDataSource %>%
          dplyr::mutate(
            cdmSourceAbbreviation = paste0(
              .data$cdmSourceAbbreviation,
              " (v",
              version$versionId,
              " ",
              as.character(version$versionDate)
            )
          )
      }
    } else {
      cdmDataSource <- cdmDataSource %>% 
        dplyr::mutate(paste0(cdmSourceAbbreviation, 
                             " version unknown"))
    }
    
    DatabaseConnector::disconnect(connection = connection)
    return(
      if (nrow(cdmDataSource) > 0) {
        tidyr::crossing(cdmDataSource, vocabulary, version) %>%
          dplyr::mutate(
            databaseDescription =
              .data$sourceDescription
          )
      } else {
        vocabulary
      }
    )
  }
