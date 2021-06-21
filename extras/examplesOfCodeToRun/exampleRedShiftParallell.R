############### Note this is a custom script, a version of CodeToRun.R that may not work for everyone ##############
############### Please use CodeToRun.R as it a more generic version  ###############################################
############### This version of code will loop over each database ids in serial                      ###############
############### processes to run Cohort Diagnostics ################################################################
# library(SkeletonCohortDiagnosticsStudy)
library(magrittr)

################################################################################
# VARIABLES - please change
################################################################################
# The folder where the study intermediate and result files will be written:
outputFolder <- "D:/studyResults/SkeletonCohortDiagnosticsStudy"
# create output directory if it does not exist
if (!dir.exists(outputFolder)) {
  dir.create(outputFolder,
             showWarnings = FALSE,
             recursive = TRUE)
}
# Optional: specify a location on your disk drive that has sufficient space.
# options(andromedaTempFolder = "s:/andromedaTemp")

############## databaseIds to run cohort diagnostics on that source  #################
databaseIds <-
  c(
    'truven_ccae',
    'truven_mdcd',
    'truven_mdcr',
    'cprd',
    'jmdc',
    'optum_extended_dod',
    'optum_ehr',
    'ims_australia_lpd',
    'ims_germany',
    'ims_france'
  )

## service name for keyring
keyringUserService <- 'OHDSI_USER'
keyringPasswordService <- 'OHDSI_PASSWORD'

# lets get meta information for each of these databaseId. This includes connection information.
source("extras/examplesOfCodeToRun/dataSourceInformation.R")


## if uploading to co-ordinator site
privateKeyFileName <- ""
siteUserName <- ""

###### create a list object that contain connection and meta information for each data source
x <- list()
for (i in (1:length(databaseIds))) {
  databaseId <- databaseIds[[i]]
  cdmSource <- cdmSources %>%
    dplyr::filter(database == databaseId)
  x[[i]] <- list(
    cdmSource = cdmSource,
    generateCohortTableName = TRUE,
    verifyDependencies = TRUE,
    databaseId = databaseId,
    outputFolder = file.path(outputFolder, databaseId),
    userService = keyringUserService,
    passwordService = keyringPasswordService,
    preMergeDiagnosticsFiles = TRUE,
    privateKeyFileName = privateKeyFileName,
    userName = siteUserName
  )
}


ParallelLogger::clusterApply(cluster = numberOfNodes,
                             x = x,
                             fun = execute)