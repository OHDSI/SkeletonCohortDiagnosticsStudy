############### Note this is a custom script, a version of CodeToRun.R that may not work for everyone ##############
############### Please use CodeToRun.R as it a more generic version  ###############################################

############### This version of code will run cohort diagnostics in parallel                      ##################

############### processes to run Cohort Diagnostics ################################################################
library(magrittr)

################################################################################
# VARIABLES - please change
################################################################################
projectCode <- "epi100"

# The folder where the study intermediate and result files will be written:
outputFolder <- "D:/studyResults/SkeletonCohortDiagnosticsStudy"
# create output directory if it does not exist
if (!dir.exists(outputFolder)) {
  dir.create(outputFolder,
             showWarnings = FALSE,
             recursive = TRUE)
}
# Optional: specify a location on your disk drive that has sufficient space.
options(andromedaTempFolder = file.path(outputFolder, "andromedaTemp"))

# lets get meta information for each of these databaseId. This includes connection information.
source("extras/examples/dataSourceInformation.R")

############## databaseIds to run cohort diagnostics on that source  #################
databaseIds <-
  c(
    'truven_ccae',
    'truven_mdcd',
    'cprd',
    'jmdc',
    'optum_extended_dod',
    'optum_ehr',
    'truven_mdcr',
    'ims_australia_lpd',
    'ims_germany',
    'ims_france',
    'iqvia_amb_emr'
    # ,
    # 'iqvia_pharmetrics_plus'
  )

## service name for keyring for db with cdm
keyringUserService <- 'OHDSI_USER'
keyringPasswordService <- 'OHDSI_PASSWORD'



###### create a list object that contain connection and meta information for each data source
x <- list()
for (i in (1:length(databaseIds))) {
  cdmSource <- cdmSources %>%
    dplyr::filter(.data$sequence == 1) %>%
    dplyr::filter(database == databaseIds[[i]])
  
  databaseId <- as.character(cdmSource$sourceKey)
  databaseName <- as.character(cdmSource$sourceName)
  databaseDescription <- as.character(cdmSource$sourceName)
  
  sourceId <- as.character(cdmSource$sourceId)
  runOn <- as.character(cdmSource$runOn)
  server <- as.character(cdmSource$serverFinal)
  cdmDatabaseSchema <-
    as.character(cdmSource$cdmDatabaseSchemaFinal)
  cohortDatabaseSchema <-
    as.character(cdmSource$cohortDatabaseSchemaFinal)
  vocabDatabaseSchema <-
    as.character(cdmSource$vocabDatabaseSchemaFinal)
  
  port <- cdmSource$port
  
  dbms <- cdmSource$dbms
  
  x[[i]] <- list(
    projectCode = projectCode,
    sourceId = sourceId,
    generateCohortTableName = TRUE,
    runOn = runOn,
    dbms = dbms,
    server = server,
    port = port,
    verifyDependencies = FALSE,
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    vocabDatabaseSchema = vocabDatabaseSchema,
    outputFolder = file.path(outputFolder, databaseId),
    userService = keyringUserService,
    passwordService = keyringPasswordService,
    preMergeDiagnosticsFiles = TRUE
  )
}

# use Parallel Logger to run in parallel
cluster <-
  ParallelLogger::makeCluster(numberOfThreads = as.integer(trunc(parallel::detectCores() /
                                                                   2)))

## file logger
loggerName <-
  paste0(
    "CDF_",
    stringr::str_replace_all(
      string = Sys.time(),
      pattern = ":|-|EDT| ",
      replacement = ''
    )
  )
loggerTrace <-
  ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))


ParallelLogger::clusterApply(cluster = cluster,
                             x = x,
                             fun = executeOnMultipleDataSources)

writeLines(readChar(paste0(loggerName, ".txt"), file.info(paste0(loggerName, ".txt"))$size))
ParallelLogger::stopCluster(cluster = cluster)
