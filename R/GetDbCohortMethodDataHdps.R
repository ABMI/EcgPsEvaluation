# @file GetDbCohortMethodDataHdps.R
#
# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of EcgPsEvaluation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

getDbCohortMethodDataHdps <- function (connectionDetails, cdmDatabaseSchema, oracleTempSchema = cdmDatabaseSchema, 
          targetId, comparatorId, outcomeIds, studyStartDate = "", 
          studyEndDate = "", exposureDatabaseSchema = cdmDatabaseSchema, 
          exposureTable = "drug_era", outcomeDatabaseSchema = cdmDatabaseSchema, 
          outcomeTable = "condition_occurrence", cdmVersion = "5", 
          excludeDrugsFromCovariates, firstExposureOnly = FALSE, removeDuplicateSubjects = FALSE, 
          restrictToCommonPeriod = FALSE, washoutPeriod = 0, maxCohortSize = 0, 
          covariateSettings, populationIds = "") 
{
  if (!missing(excludeDrugsFromCovariates)) {
    warning("The excludeDrugsFromCovariates argument has been deprecated. Please explicitly exclude the drug concepts in the covariate settings")
  }
  else {
    excludeDrugsFromCovariates = FALSE
  }
  if (is.null(studyStartDate)) {
    studyStartDate <- ""
  }
  if (is.null(studyEndDate)) {
    studyEndDate <- ""
  }
  if (studyStartDate != "" && regexpr("^[12][0-9]{3}[01][0-9][0-3][0-9]$", 
                                      studyStartDate) == -1) 
    stop("Study start date must have format YYYYMMDD")
  if (studyEndDate != "" && regexpr("^[12][0-9]{3}[01][0-9][0-3][0-9]$", 
                                    studyEndDate) == -1) 
    stop("Study end date must have format YYYYMMDD")
  if (is.logical(removeDuplicateSubjects)) {
    if (removeDuplicateSubjects) 
      removeDuplicateSubjects <- "remove all"
    else removeDuplicateSubjects <- "keep all"
  }
  if (!(removeDuplicateSubjects %in% c("keep all", "keep first", 
                                       "remove all"))) 
    stop("removeDuplicateSubjects should have value \"keep all\", \"keep first\", or \"remove all\".")
  ParallelLogger::logTrace("Getting cohort method data for target ID ", 
                           targetId, " and comparator ID ", comparatorId)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  if (excludeDrugsFromCovariates) {
    if (exposureTable != "drug_era") 
      warning("Removing drugs from covariates, but not sure if exposure IDs are valid drug concepts")
    sql <- "SELECT descendant_concept_id FROM @cdm_database_schema.concept_ancestor WHERE ancestor_concept_id IN (@target_id, @comparator_id)"
    sql <- SqlRender::render(sql = sql, cdm_database_schema = cdmDatabaseSchema, 
                             target_id = targetId, comparator_id = comparatorId)
    sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
    conceptIds <- DatabaseConnector::querySql(connection, 
                                              sql, snakeCaseToCamelCase = TRUE)
    conceptIds <- conceptIds$descendantConceptId
    ParallelLogger::logDebug("Excluding concept Ids from covariates: ", 
                             paste(conceptIds, collapse = ", "))
    if (is(covariateSettings, "covariateSettings")) {
      covariateSettings$excludedCovariateConceptIds <- c(covariateSettings$excludedCovariateConceptIds, 
                                                         conceptIds)
    }
    else if (is.list(covariateSettings)) {
      for (i in 1:length(covariateSettings)) {
        covariateSettings[[i]]$excludedCovariateConceptIds <- c(covariateSettings[[i]]$excludedCovariateConceptIds, 
                                                                conceptIds)
      }
    }
  }
  ParallelLogger::logInfo("Constructing target and comparator cohorts")
  renderedSql <- SqlRender::loadRenderTranslateSql("CreateCohorts.sql", 
                                                   packageName = "CohortMethod", dbms = connectionDetails$dbms, 
                                                   oracleTempSchema = oracleTempSchema, cdm_database_schema = cdmDatabaseSchema, 
                                                   exposure_database_schema = exposureDatabaseSchema, exposure_table = exposureTable, 
                                                   cdm_version = cdmVersion, target_id = targetId, comparator_id = comparatorId, 
                                                   study_start_date = studyStartDate, study_end_date = studyEndDate, 
                                                   first_only = firstExposureOnly, remove_duplicate_subjects = removeDuplicateSubjects, 
                                                   washout_period = washoutPeriod, restrict_to_common_period = restrictToCommonPeriod)
  DatabaseConnector::executeSql(connection, renderedSql)
  sampled <- FALSE
  if (maxCohortSize != 0) {
    renderedSql <- SqlRender::loadRenderTranslateSql("CountCohorts.sql", 
                                                     packageName = "CohortMethod", dbms = connectionDetails$dbms, 
                                                     oracleTempSchema = oracleTempSchema, cdm_version = cdmVersion, 
                                                     target_id = targetId)
    counts <- DatabaseConnector::querySql(connection, renderedSql, 
                                          snakeCaseToCamelCase = TRUE)
    ParallelLogger::logDebug("Pre-sample total row count is ", 
                             sum(counts$rowCount))
    preSampleCounts <- tibble::tibble(dummy = 0)
    idx <- which(counts$treatment == 1)
    if (length(idx) == 0) {
      preSampleCounts$targetPersons = 0
      preSampleCounts$targetExposures = 0
    }
    else {
      preSampleCounts$targetPersons = counts$personCount[idx]
      preSampleCounts$targetExposures = counts$rowCount[idx]
    }
    idx <- which(counts$treatment == 0)
    if (length(idx) == 0) {
      preSampleCounts$comparatorPersons = 0
      preSampleCounts$comparatorExposures = 0
    }
    else {
      preSampleCounts$comparatorPersons = counts$personCount[idx]
      preSampleCounts$comparatorExposures = counts$rowCount[idx]
    }
    preSampleCounts$dummy <- NULL
    if (preSampleCounts$targetExposures > maxCohortSize) {
      ParallelLogger::logInfo("Downsampling target cohort from ", 
                              preSampleCounts$targetExposures, " to ", maxCohortSize)
      sampled <- TRUE
    }
    if (preSampleCounts$comparatorExposures > maxCohortSize) {
      ParallelLogger::logInfo("Downsampling comparator cohort from ", 
                              preSampleCounts$comparatorExposures, " to ", 
                              maxCohortSize)
      sampled <- TRUE
    }
    if (sampled) {
      renderedSql <- SqlRender::loadRenderTranslateSql("SampleCohorts.sql", 
                                                       packageName = "CohortMethod", dbms = connectionDetails$dbms, 
                                                       oracleTempSchema = oracleTempSchema, cdm_version = cdmVersion, 
                                                       max_cohort_size = maxCohortSize)
      DatabaseConnector::executeSql(connection, renderedSql)
    }
  }
  ParallelLogger::logInfo("Fetching cohorts from server")
  start <- Sys.time()
  cohortSql <- SqlRender::loadRenderTranslateSql("GetCohorts.sql", 
                                                 packageName = "CohortMethod", dbms = connectionDetails$dbms, 
                                                 oracleTempSchema = oracleTempSchema, cdm_version = cdmVersion, 
                                                 target_id = targetId, sampled = sampled)
  cohorts <- DatabaseConnector::querySql(connection, cohortSql, 
                                         snakeCaseToCamelCase = TRUE)
  ParallelLogger::logDebug("Fetched cohort total rows in target is ", 
                           sum(cohorts$treatment), ", total rows in comparator is ", 
                           sum(!cohorts$treatment))
  if (nrow(cohorts) == 0) {
    warning("Target and comparator cohorts are empty")
  }
  else if (sum(cohorts$treatment == 1) == 0) {
    warning("Target cohort is empty")
  }
  else if (sum(cohorts$treatment == 0) == 0) {
    warning("Comparator cohort is empty")
  }
  metaData <- list(targetId = targetId, comparatorId = comparatorId, 
                   studyStartDate = studyStartDate, studyEndDate = studyEndDate)
  if (firstExposureOnly || removeDuplicateSubjects != "keep all" || 
      washoutPeriod != 0) {
    rawCountSql <- SqlRender::loadRenderTranslateSql("CountOverallExposedPopulation.sql", 
                                                     packageName = "CohortMethod", dbms = connectionDetails$dbms, 
                                                     oracleTempSchema = oracleTempSchema, cdm_database_schema = cdmDatabaseSchema, 
                                                     exposure_database_schema = exposureDatabaseSchema, 
                                                     exposure_table = tolower(exposureTable), cdm_version = cdmVersion, 
                                                     target_id = targetId, comparator_id = comparatorId, 
                                                     study_start_date = studyStartDate, study_end_date = studyEndDate)
    rawCount <- DatabaseConnector::querySql(connection, 
                                            rawCountSql, snakeCaseToCamelCase = TRUE)
    if (nrow(rawCount) == 0) {
      counts <- tibble::tibble(description = "Original cohorts", 
                               targetPersons = 0, comparatorPersons = 0, targetExposures = 0, 
                               comparatorExposures = 0)
    }
    else {
      counts <- tibble::tibble(description = "Original cohorts", 
                               targetPersons = rawCount$exposedCount[rawCount$treatment == 
                                                                       1], comparatorPersons = rawCount$exposedCount[rawCount$treatment == 
                                                                                                                       0], targetExposures = rawCount$exposureCount[rawCount$treatment == 
                                                                                                                                                                      1], comparatorExposures = rawCount$exposureCount[rawCount$treatment == 
                                                                                                                                                                                                                         0])
    }
    metaData$attrition <- counts
    label <- c()
    if (firstExposureOnly) {
      label <- c(label, "first exp. only")
    }
    if (removeDuplicateSubjects == "remove all") {
      label <- c(label, "removed subs in both cohorts")
    }
    else if (removeDuplicateSubjects == "keep first") {
      label <- c(label, "first cohort only")
    }
    if (restrictToCommonPeriod) {
      label <- c(label, "restrict to common period")
    }
    if (washoutPeriod) {
      label <- c(label, paste(washoutPeriod, "days of obs. prior"))
    }
    label <- paste(label, collapse = " & ")
    substring(label, 1) <- toupper(substring(label, 1, 1))
    if (sampled) {
      preSampleCounts$description <- label
      metaData$attrition <- rbind(metaData$attrition, 
                                  preSampleCounts)
      metaData$attrition <- rbind(metaData$attrition, 
                                  getCounts(cohorts, "Random sample"))
    }
    else {
      metaData$attrition <- rbind(metaData$attrition, 
                                  getCounts(cohorts, label))
    }
  }
  else {
    if (sampled) {
      preSampleCounts$description <- "Original cohorts"
      metaData$attrition <- preSampleCounts
      metaData$attrition <- rbind(metaData$attrition, 
                                  getCounts(cohorts, "Random sample"))
    }
    else {
      metaData$attrition <- getCounts(cohorts, "Original cohorts")
    }
  }
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Fetching cohorts took ", signif(delta, 
                                                           3), " ", attr(delta, "units"))
  if (sampled) {
    cohortTable <- "#cohort_sample"
  }
  else {
    cohortTable <- "#cohort_person"
  }
  covariateData <- FeatureExtraction::getDbCovariateData(connection = connection, 
                                                         oracleTempSchema = oracleTempSchema, cdmDatabaseSchema = cdmDatabaseSchema, 
                                                         cdmVersion = cdmVersion, cohortTable = cohortTable, 
                                                         cohortTableIsTemp = TRUE, rowIdField = "row_id", covariateSettings = covariateSettings)
  #ParallelLogger::logDebug("Fetched covariates total count is ", 
  #                         covariateData$covariates %>% count() %>% pull())
  ParallelLogger::logInfo("Fetching outcomes from server")
  start <- Sys.time()
  outcomeSql <- SqlRender::loadRenderTranslateSql("GetOutcomes.sql", 
                                                  packageName = "CohortMethod", dbms = connectionDetails$dbms, 
                                                  oracleTempSchema = oracleTempSchema, cdm_database_schema = cdmDatabaseSchema, 
                                                  outcome_database_schema = outcomeDatabaseSchema, outcome_table = outcomeTable, 
                                                  outcome_ids = outcomeIds, cdm_version = cdmVersion, 
                                                  sampled = sampled)
  outcomes <- DatabaseConnector::querySql(connection, outcomeSql, 
                                          snakeCaseToCamelCase = TRUE)
  metaData$outcomeIds = outcomeIds
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Fetching outcomes took ", signif(delta, 
                                                            3), " ", attr(delta, "units"))
  ParallelLogger::logDebug("Fetched outcomes total count is ", 
                           nrow(outcomes))
  renderedSql <- SqlRender::loadRenderTranslateSql("RemoveCohortTempTables.sql", 
                                                   packageName = "CohortMethod", dbms = connectionDetails$dbms, 
                                                   oracleTempSchema = oracleTempSchema, sampled = sampled)
  DatabaseConnector::executeSql(connection, renderedSql, progressBar = FALSE, 
                                reportOverallTime = FALSE)
  covariateData$cohorts <- cohorts
  covariateData$outcomes <- outcomes
  attr(covariateData, "metaData") <- append(attr(covariateData, 
                                                 "metaData"), metaData)
  class(covariateData) <- "CohortMethodData"
  attr(class(covariateData), "package") <- "CohortMethod"
  return(covariateData)
}


getCounts <- function(population, description = "") {
  targetPersons <- length(unique(population$personSeqId[population$treatment == 1]))
  comparatorPersons <- length(unique(population$personSeqId[population$treatment == 0]))
  targetExposures <- length(population$personSeqId[population$treatment == 1])
  comparatorExposures <- length(population$personSeqId[population$treatment == 0])
  counts <- dplyr::tibble(description = description,
                          targetPersons = targetPersons,
                          comparatorPersons = comparatorPersons,
                          targetExposures = targetExposures,
                          comparatorExposures = comparatorExposures)
  return(counts)
}