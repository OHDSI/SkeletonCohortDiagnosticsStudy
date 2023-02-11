# this script enables to easily remove log entries in incremental mode.
# Please use this script, if we want to rerun/overwrite previous data

logFolder <-
  "s:/SkeletonCohortDiagnosticsStudy"
diagnosticsFileName <- "CreatedDiagnostics.csv"

listFiles <-
  list.files(
    path = logFolder,
    pattern = diagnosticsFileName,
    full.names = TRUE,
    recursive = TRUE
  )

# options for tasks to remove
# "runInclusionStatistics", "runIncludedSourceConcepts ", "runOrphanConcepts",
# "runTimeSeries", "runVisitContext",
# "runBreakdownIndexEvents", "runIncidenceRate", "runCohortRelationship","runTemporalCohortCharacterization"


# tasksToRemove <- c("runTimeSeries")


for (i in (1:length(listFiles))) {
  readr::read_csv(
    file = listFiles[[i]],
    col_types = readr::cols(),
    guess_max = min(1e7)
  ) |>
    dplyr::filter(!.data$task %in% tasksToRemove) |>
    readr::write_excel_csv(file = listFiles[[i]])
}
