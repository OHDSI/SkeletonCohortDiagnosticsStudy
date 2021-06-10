############### Note this is a custom script, a version of CodeToRun.R that may not work for everyone ##############
############### Please use CodeToRun.R as it a more generic version  ###############################################
library(SkeletonCohortDiagnosticsStudy)
library(magrittr)

# Optional: specify where the temporary files (used by the Andromeda package) will be created:
# This is optional, as andromeda is able to assign temporary location using your Operating Systems (OS) settings, 
# but sometimes the temporary location specified by your OS may not have sufficient storage space.
# To avoid such scenarios, it maybe useful to change and uncomment the line below to point to 
# a location on your disk drive that has sufficient space.
# options(andromedaTempFolder = "s:/andromedaTemp")

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

################################################################################
# VARIABLES - please change
################################################################################
# The folder where the study intermediate and result files will be written:
outputFolder <- "D:/studyResults/SkeletonCohortDiagnosticsStudy"

# create output directory if it doesnt exist
if (!dir.exists(outputFolder)) {
  dir.create(outputFolder, showWarnings = FALSE, recursive = TRUE)
}

################################################################################
# WORK
################################################################################

# the next set of lines shows how one site uses ROhdsiWebApi to get cdmSources configuration
# then parses it to collect the connectionSpecification information into
# one object called cdmSources
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
  dplyr::mutate(server = tolower(paste0(Sys.getenv("serverRoot"),"/", .data$database)))

# the cdm sources is then used to populate connectionSpecifications object
connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_mdcd')

# Details for connecting to the server:
connectionDetails <-
  DatabaseConnector::createConnectionDetails(
    dbms = connectionSpecifications$dbms,
    server = connectionSpecifications$server,
    user = keyring::key_get(service = 'OHDSI_USER'),
    password =  keyring::key_get(service = 'OHDSI_PASSWORD'),
    port = connectionSpecifications$port
  )

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- connectionSpecifications$cdmDatabaseSchema

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <-
  paste0("scratch_", keyring::key_get(service = userNameService))
# cohortDatabaseSchema <-
#   paste0("scratch_rao_", connectionSpecifications$database)

cohortTable <-
  paste0("s",
         connectionSpecifications$sourceId,
         "_",
         "SkeletonCohortDiagnosticsStudy")

# Some meta-information that will be used by the export function:
databaseId <- connectionSpecifications$database

# this function calls details of the data source.

getDataSourceInformation <-
  function(connectionDetails,
           cdmDatabaseSchema,
           vocabDatabaseSchema) {
    connection <-
      DatabaseConnector::connect(connectionDetails = connectionDetails)
    sqlCdmDataSource <- "select * from @cdmDatabaseSchema.cdm_source;"
    sqlVocabularyVersion <-
      "select * from @vocabDatabaseSchema.vocabulary where vocabulary_id = 'None';"
    
    cdmDataSource <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sqlCdmDataSource,
        cdmDatabaseSchema = cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble() %>%
      dplyr::rename(vocabularyVersionCdm = .data$vocabularyVersion)
    
    vocabulary <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sqlVocabularyVersion,
        vocabDatabaseSchema = vocabDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble() %>%
      dplyr::rename(vocabularyVersion = .data$vocabularyVersion)
    
    DatabaseConnector::disconnect(connection = connection)
    return((
      if (nrow(cdmDataSource) > 0) {
        tidyr::crossing(cdmDataSource, vocabulary) %>%
          dplyr::mutate(
            databaseDescription =
              .data$sourceDescription
          )
      } else {
        vocabulary
      }
    ))
  }

dataSourceInformation <- getDataSourceInformation(connectionDetails = connectionDetails,
                                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                                  vocabDatabaseSchema = connectionSpecifications$vocabDatabaseSchema)


databaseName <- dataSourceInformation$cdmSourceName
databaseDescription <- dataSourceInformation$sourceDescription

execute(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  verifyDependencies = TRUE,
  outputFolder = outputFolder,
  databaseId = databaseId,
  databaseName = databaseName,
  databaseDescription = databaseDescription
)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)


# Upload the results to the OHDSI SFTP server:
privateKeyFileName <- ""
userName <- ""
uploadResults(outputFolder, privateKeyFileName, userName)
