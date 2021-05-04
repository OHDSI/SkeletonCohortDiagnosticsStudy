############### Note this is a custom script, a version of CodeToRun.R that may not work for everyone ##############
############### Please use CodeToRun.R as it a more generic version  ###############################################

source(Sys.getenv("startUpScriptLocation"))
connectionSpecifications <- cdmSources %>% dplyr::filter(sequence == 1) %>% dplyr::filter(database ==
                                                                                            "truven_ccae")

library(SkeletonCohortDiagnosticsStudy)

# The folder where the study intermediate and result files will be written:
outputFolder <-
        file.path("D:/temp/", connectionSpecifications$database)
# unlink(x = outputFolder, recursive = TRUE, force = TRUE)
dir.create(path = outputFolder, showWarnings = TRUE, recursive = TRUE)

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

dbms <- connectionSpecifications$dbms
port <- connectionSpecifications$port
server <- connectionSpecifications$server
cdmDatabaseSchema <- connectionSpecifications$cdmDatabaseSchema
vocabDatabaseSchema <- connectionSpecifications$vocabDatabaseSchema
databaseId <- connectionSpecifications$database
userNameService <- "OHDSI_USER"
passwordService <- "OHDSI_PASSWORD"

cohortDatabaseSchema <-
        paste0("scratch_", keyring::key_get(service = userNameService))
connectionDetails <-
        DatabaseConnector::createConnectionDetails(
                dbms = dbms,
                user = keyring::key_get(service = userNameService),
                password = keyring::key_get(service = passwordService),
                port = port,
                server = server
        )

cohortTable <-
        paste0("s",
               connectionSpecifications$sourceId,
               "_",
               "SkeletonCohortDiagnosticsStudy")

dataSouceInformation <- getDataSourceInformation(connectionDetails = connectionDetails,
                                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                                 vocabDatabaseSchema = vocabDatabaseSchema)

execute(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        outputFolder = outputFolder,
        databaseId = databaseId,
        databaseName = dataSouceInformation$cdmSourceName,
        databaseDescription = dataSouceInformation$sourceDescription
)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)
CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)



# Upload the results to the OHDSI SFTP server:
privateKeyFileName <- ""
userName <- ""
SkeletonCohortDiagnosticsStudy::uploadResults(outputFolder, privateKeyFileName, userName)


# connectionDetailsToUpload <-
#         createConnectionDetails(
#                 dbms = 'postgresql',
#                 server = paste(
#                         Sys.getenv('shinydbServer'),
#                         Sys.getenv('shinydbDatabase'),
#                         sep = '/'
#                 ),
#                 port = Sys.getenv('shinydbPort'),
#                 user = Sys.getenv('shinyDbUserGowtham'),
#                 password = Sys.getenv('shinyDbPasswordGowtham')
#         )
#
# resultsSchema <- 'examplePackageCdTruven'
#
# createResultsDataModel(connectionDetails = connectionDetailsToUpload, schema = resultsSchema)
#
# path = outputFolder
#
# zipFilesToUpload <- list.files(
#         path = path,
#         pattern = '.zip',
#         recursive = TRUE,
#         full.names = TRUE
# )
#
# for (i in (1:length(zipFilesToUpload))) {
#         CohortDiagnostics::uploadResults(
#                 connectionDetails = connectionDetailsToUpload,
#                 schema = resultsSchema,
#                 zipFileName = zipFilesToUpload[[i]]
#         )
# }
