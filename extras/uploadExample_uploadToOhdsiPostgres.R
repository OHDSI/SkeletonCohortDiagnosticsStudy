library(magrittr)
# where are the cohort diagnostics output?
folderWithZipFilesToUpload <-
  "D:\\studyResults\\ohdsiTutorial2022CohortDiagnostics\\iquvia5pct"

# what is the name of the schema you want to upload to?
resultsSchema <-
  tolower('ohdsi22TutorialCd') # change to your schema - please use lower case

# Postgres server: connection details to OHDSI Phenotype library. Please change to your postgres connection details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = Sys.getenv("shinydbDbms", unset = "postgresql"),
  server = paste(
    Sys.getenv("shinydbServer"),
    Sys.getenv("shinydbDatabase"),
    sep = "/"
  ),
  port = Sys.getenv("shinydbPort"),
  user = Sys.getenv("shinydbUser"),
  password = Sys.getenv("shinydbPW")
)

connection <-
  DatabaseConnector::connect(connectionDetails = connectionDetails)

# check if schema exists in database
schemaExists <-
  DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT exists(
                         select schema_name
                         FROM information_schema.schemata
                         WHERE schema_name = '@results_database_schema'
                         );",
    results_database_schema = resultsSchema
  ) %>%
  dplyr::tibble() %>%
  dplyr::mutate(exists = dplyr::case_when(toupper(.data$EXISTS) == 'T' ~ TRUE,
                                          TRUE ~ FALSE)) %>%
  dplyr::pull(.data$exists)

tablesInResultsDataModel <-
  CohortDiagnostics::getResultsDataModelSpecifications() %>%
  dplyr::select(.data$tableName) %>%
  dplyr::distinct() %>%
  dplyr::pull()

if (!schemaExists) {
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "select create_schema('@results_database_schema');",
    results_database_schema = resultsSchema
  )
  CohortDiagnostics::createResultsDataModel(connection = connection, schema = resultsSchema)
} else {
  # back up annotation tables
  annotationTables <-
    tablesInResultsDataModel[stringr::str_detect(string = tablesInResultsDataModel,
                                                 pattern = "annotation")]
  
  for (i in (1:length(annotationTables))) {
    writeLines(paste0("Backing up ", annotationTables[[i]]))
    
    tablesInSchema <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT DISTINCT table_name FROM information_schema.tables where table_schema = '@results_database_schema';",
        results_database_schema = resultsSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::pull(.data$tableName) %>%
      tolower()
    
    if (annotationTables[[i]] %in% tablesInSchema) {
      data <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM @results_database_schema.@annotation_table;",
        results_database_schema = resultsSchema,
        annotation_table = annotationTables[[i]],
        snakeCaseToCamelCase = TRUE
      )  %>%
        dplyr::arrange(1) %>%
        dplyr::tibble()
      assign(x = annotationTables[[i]],
             value = data)
      readr::write_excel_csv(
        x = get(annotationTables[[i]]),
        file = file.path(
          folderWithZipFilesToUpload,
          paste0(annotationTables[[i]], ".csv")
        ),
        append = TRUE,
        col_names = TRUE
      )
    }
  }
  
  for (i in (1:length(tablesInResultsDataModel))) {
    writeLines(paste0("Dropping table ", tablesInResultsDataModel[[i]]))
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DROP TABLE IF EXISTS @database_schema.@table_name CASCADE;",
      database_schema = resultsSchema,
      table_name = tablesInResultsDataModel[[i]]
    )
  }
  CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)
}

# sqlGrant <-
#   "grant select on all tables in schema @results_database_schema to phenotypelibrary;"
# DatabaseConnector::renderTranslateExecuteSql(
#   connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#   sql = sqlGrant,
#   results_database_schema = resultsSchema
# )

# trying to keep track of files that were recently uploaded in current R session
if (exists("listOfZipFilesToUpload")) {
  listOfZipFilesToUpload2 <- listOfZipFilesToUpload
  if (!exists("listOfZipFilesToUpload2")) {
    listOfZipFilesToUpload2 <- c()
  }
} else {
  listOfZipFilesToUpload <- c()
  listOfZipFilesToUpload2 <- c()
}

listOfZipFilesToUpload <-
  list.files(
    path = folderWithZipFilesToUpload,
    pattern = ".zip",
    full.names = TRUE,
    recursive = TRUE
  )

listOfZipFilesToUpload <-
  setdiff(listOfZipFilesToUpload, listOfZipFilesToUpload2)
for (i in (1:length(listOfZipFilesToUpload))) {
  CohortDiagnostics::uploadResults(
    connectionDetails = connectionDetails,
    schema = resultsSchema,
    zipFileName = listOfZipFilesToUpload[[i]]
  )
}
listOfZipFilesToUpload2 <-
  c(listOfZipFilesToUpload, listOfZipFilesToUpload2) %>% unique() %>% sort()


if (all(exists(annotationTables), length(annotationTables) > 0)) {
  # reupload annotation tables
  for (i in (1:length(annotationTables))) {
    if (!exists(annotationTables[[i]])) {
      if (file.exists(file.path(
        folderWithZipFilesToUpload,
        paste0(annotationTables[[i]], ".csv")
      ))) {
        data <-
          readr::read_csv(
            file = file.path(
              folderWithZipFilesToUpload,
              paste0(annotationTables[[i]], ".csv")
            ),
            col_types = readr::cols()
          )
        assign(x = annotationTables[[i]],
               value = data)
      } else {
        writeLines(paste0(
          "Annotation table not found: ",
          paste0(annotationTables[[i]], ".csv")
        ))
      }
    }
    
    if (exists(annotationTables[[i]])) {
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = "DELETE FROM @results_database_schema.@annotation_table;",
        results_database_schema = resultsSchema,
        annotation_table = annotationTables[[i]]
      )
      DatabaseConnector::insertTable(
        connection = connection,
        databaseSchema = resultsSchema,
        tableName = annotationTables[[i]],
        dropTableIfExists = FALSE,
        createTable = FALSE,
        data = get(annotationTables[[i]]) %>% dplyr::distinct(),
        tempTable = FALSE,
        bulkLoad = (Sys.getenv("bulkLoad") == TRUE),
        camelCaseToSnakeCase = TRUE
      )
    }
  }
}

# Maintenance
connection <-
  DatabaseConnector::connect(connectionDetails = connectionDetails)
for (i in (1:length(tablesInResultsDataModel))) {
  # vacuum
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "VACUUM VERBOSE ANALYZE @database_schema.@table_name;",
    database_schema = resultsSchema,
    table_name = tablesInResultsDataModel[[i]]
  )
}
