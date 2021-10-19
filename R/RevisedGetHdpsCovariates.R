# @file RevisedGetHdpsCovariates.R
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

#' Get HDPS covariate information from the database
#'
#' @description
#' Constructs the set of covariates for one or more cohorts using data in the CDM schema. This
#' implements the covariates typically used in the HDPS algorithm.
#'
#' @param covariateSettings   An object of type \code{covariateSettings} as created using the
#'                            \code{\link{createHdpsCovariateSettings}} function.
#'
#' @template GetCovarParams
#'
#' @export
getDbHdpsCovariateDataHdps <- function(connection,
                                   oracleTempSchema = NULL,
                                   cdmDatabaseSchema,
                                   cohortTable = "cohort_person",
                                   cohortId = -1,
                                   cdmVersion = "5",
                                   rowIdField = "subject_id",
                                   covariateSettings,
                                   aggregated = FALSE,
                                   populationIds = "608, 609") {
  if (cohortId != -1)
    stop("Haven't implemented restricting to cohort ID yet.")
  if (aggregated)
    stop("Aggregation not implemented yet")
  if (cdmVersion == "4")
    stop("Common Data Model version 4 is not supported")
  writeLines("Constructing HDPS covariates")
  cdmVersion <- "5"
  cdmDatabase <- strsplit(cdmDatabaseSchema, "\\.")[[1]][1]
  
  if (cdmVersion == "4") {
    cohortDefinitionId <- "cohort_concept_id"
    conceptClassId <- "concept_class"
    measurement <- "observation"
  } else {
    cohortDefinitionId <- "cohort_definition_id"
    conceptClassId <- "concept_class_id"
    measurement <- "measurement"
  }
  
  if (is.null(covariateSettings$excludedCovariateConceptIds) || length(covariateSettings$excludedCovariateConceptIds) ==
      0) {
    hasExcludedCovariateConceptIds <- FALSE
  } else {
    if (!is.numeric(covariateSettings$excludedCovariateConceptIds))
      stop("excludedCovariateConceptIds must be a (vector of) numeric")
    hasExcludedCovariateConceptIds <- TRUE
    DatabaseConnector::insertTable(connection,
                                   tableName = "#excluded_cov",
                                   data = tibble(concept_id = as.integer(covariateSettings$excludedCovariateConceptIds)),
                                   dropTableIfExists = TRUE,
                                   createTable = TRUE,
                                   tempTable = TRUE,
                                   oracleTempSchema = oracleTempSchema)
  }
  
  if (is.null(covariateSettings$includedCovariateConceptIds) || length(covariateSettings$includedCovariateConceptIds) ==
      0) {
    hasIncludedCovariateConceptIds <- FALSE
  } else {
    if (!is.numeric(covariateSettings$includedCovariateConceptIds))
      stop("includedCovariateConceptIds must be a (vector of) numeric")
    hasIncludedCovariateConceptIds <- TRUE
    DatabaseConnector::insertTable(connection,
                                   tableName = "#included_cov",
                                   data = tibble(concept_id = as.integer(covariateSettings$includedCovariateConceptIds)),
                                   dropTableIfExists = TRUE,
                                   createTable = TRUE,
                                   tempTable = TRUE,
                                   oracleTempSchema = oracleTempSchema)
  }
  
  renderedSql <- SqlRender::loadRenderTranslateSql("GetHdpsCovariates.sql",
                                                   packageName = "FeatureExtraction",
                                                   dbms = attr(connection, "dbms"),
                                                   oracleTempSchema = oracleTempSchema,
                                                   cdm_database = cdmDatabase,
                                                   cdm_version = cdmVersion,
                                                   cohort_temp_table = cohortTable,
                                                   row_id_field = rowIdField,
                                                   cohort_definition_i = cohortDefinitionId,
                                                   concept_class_id = conceptClassId,
                                                   measurement = measurement,
                                                   use_covariate_cohort_id_is_1 = covariateSettings$useCovariateCohortIdIs1,
                                                   use_covariate_demographics = covariateSettings$useCovariateDemographics,
                                                   use_covariate_demographics_gender = covariateSettings$useCovariateDemographicsGender,
                                                   use_covariate_demographics_race = covariateSettings$useCovariateDemographicsRace,
                                                   use_covariate_demographics_ethnicity = covariateSettings$useCovariateDemographicsEthnicity,
                                                   use_covariate_demographics_age = covariateSettings$useCovariateDemographicsAge,
                                                   use_covariate_demographics_year = covariateSettings$useCovariateDemographicsYear,
                                                   use_covariate_demographics_month = covariateSettings$useCovariateDemographicsMonth,
                                                   use_covariate_condition_occurrence = covariateSettings$useCovariateConditionOccurrence,
                                                   use_covariate_3_digit_icd_9_inpatient_180d = covariateSettings$useCovariate3DigitIcd9Inpatient180d,
                                                   use_covariate_3_digit_icd_9_inpatient_180d_med_f = covariateSettings$useCovariate3DigitIcd9Inpatient180dMedF,
                                                   use_covariate_3_digit_icd_9_inpatient_180d_75_f = covariateSettings$useCovariate3DigitIcd9Inpatient180d75F,
                                                   use_covariate_3_digit_icd_9_ambulatory_180d = covariateSettings$useCovariate3DigitIcd9Ambulatory180d,
                                                   use_covariate_3_digit_icd_9_ambulatory_180d_med_f = covariateSettings$useCovariate3DigitIcd9Ambulatory180dMedF,
                                                   use_covariate_3_digit_icd_9_ambulatory_180d_75_f = covariateSettings$useCovariate3DigitIcd9Ambulatory180d75F,
                                                   use_covariate_drug_exposure = covariateSettings$useCovariateDrugExposure,
                                                   use_covariate_ingredient_exposure_180d = covariateSettings$useCovariateIngredientExposure180d,
                                                   use_covariate_ingredient_exposure_180d_med_f = covariateSettings$useCovariateIngredientExposure180dMedF,
                                                   use_covariate_ingredient_exposure_180d_75_f = covariateSettings$useCovariateIngredientExposure180d75F,
                                                   use_covariate_procedure_occurrence = covariateSettings$useCovariateProcedureOccurrence,
                                                   use_covariate_inpatient_procedure_occurrence_180d = covariateSettings$useCovariateProcedureOccurrenceInpatient180d,
                                                   use_covariate_inpatient_procedure_occurrence_180d_med_f = covariateSettings$useCovariateProcedureOccurrenceInpatient180dMedF,
                                                   use_covariate_inpatient_procedure_occurrence_180d_75_f = covariateSettings$useCovariateProcedureOccurrenceInpatient180d75F,
                                                   use_covariate_ambulatory_procedure_occurrence_180d = covariateSettings$useCovariateProcedureOccurrenceAmbulatory180d,
                                                   use_covariate_ambulatory_procedure_occurrence_180d_med_f = covariateSettings$useCovariateProcedureOccurrenceAmbulatory180dMedF,
                                                   use_covariate_ambulatory_procedure_occurrence_180d_75_f = covariateSettings$useCovariateProcedureOccurrenceAmbulatory180d75F,
                                                   has_excluded_covariate_concept_ids = hasExcludedCovariateConceptIds,
                                                   has_included_covariate_concept_ids = hasIncludedCovariateConceptIds,
                                                   delete_covariates_small_count = covariateSettings$deleteCovariatesSmallCount)
  
  DatabaseConnector::executeSql(connection, renderedSql)
  writeLines("Done")
  
  writeLines("Fetching data from server")
  start <- Sys.time()
  covariateSql <- "SELECT row_id, covariate_id, covariate_value FROM #cov ORDER BY covariate_id, row_id"
  covariateSql <- SqlRender::translate(sql = covariateSql,
                                       targetDialect = attr(connection, "dbms"),
                                       oracleTempSchema = oracleTempSchema)
  covariates <- querySql.ffdf(connection, covariateSql)
  
  covariateRefSql <- "SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM #cov_ref ORDER BY covariate_id"
  covariateRefSql <- SqlRender::translate(sql = covariateRefSql,
                                          targetDialect = attr(connection, "dbms"),
                                          oracleTempSchema = oracleTempSchema)
  covariateRef <- querySql.ffdf(connection, covariateRefSql)
  
  sql <- "SELECT COUNT_BIG(*) FROM @cohort_temp_table WHERE cohort_definition_id in (@population_ids)"
  sql <- SqlRender::render(sql, cohort_temp_table = cohortTable, population_ids = populationIds)
  sql <- SqlRender::translate(sql = sql,
                              targetDialect = attr(connection, "dbms"),
                              oracleTempSchema = oracleTempSchema)
  populationSize <- DatabaseConnector::querySql(connection, sql)[1, 1]
  
  delta <- Sys.time() - start
  writeLines(paste("Fetching data took", signif(delta, 3), attr(delta, "units")))
  
  renderedSql <- SqlRender::loadRenderTranslateSql("RemoveCovariateTempTables.sql",
                                                   packageName = "FeatureExtraction",
                                                   dbms = attr(connection, "dbms"),
                                                   oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection,
                                renderedSql,
                                progressBar = FALSE,
                                reportOverallTime = FALSE)
  
  colnames(covariates) <- SqlRender::snakeCaseToCamelCase(colnames(covariates))
  colnames(covariateRef) <- SqlRender::snakeCaseToCamelCase(colnames(covariateRef))
  
  # Remove redundant covariates
  writeLines("Removing redundant covariates")
  start <- Sys.time()
  deletedCovariateIds <- c()
  if (nrow(covariates) != 0) {
    # First delete all single covariates that appear in every row with the same value
    #valueCounts <- bySumFf(ff::ff(1, length = nrow(covariates)), covariates$covariateId)
    valueCounts <- data.frame(bins=length(covariates$covariateId), sums = as.data.frame(as.ffdf(covariates$covariateId)))
    nonSparseIds <- valueCounts$bins[valueCounts$sums == populationSize]
    for (covariateId in nonSparseIds) {
      selection <- covariates$covariateId == covariateId
      idx <- ffbase::ffwhich(selection, selection == TRUE)
      values <- ffbase::unique.ff(covariates$covariateValue[idx])
      if (length(values) == 1) {
        idx <- ffbase::ffwhich(selection, selection == FALSE)
        covariates <- covariates[idx, ]
        deletedCovariateIds <- c(deletedCovariateIds, covariateId)
      }
    }
    # Next, from groups of covariates that together cover every row, remove the most prevalence one
    problematicAnalysisIds <- c(2, 3, 4, 5, 6, 7)  # Gender, race, ethnicity, age, year, month
    for (analysisId in problematicAnalysisIds) {
      t <- covariateRef$analysisId == analysisId
      if (ffbase::sum.ff(t) != 0) {
        covariateIds <- ff::as.ram(covariateRef$covariateId[ffbase::ffwhich(t, t == TRUE)])
        freq <- sapply(covariateIds, function(x) {
          ffbase::sum.ff(covariates$covariateId == x)
        })
        if (sum(freq) == populationSize) {
          # Each row belongs to one of the categories, making one redunant. Remove most prevalent one
          categoryToDelete <- covariateIds[which(freq == max(freq))[1]]
          deletedCovariateIds <- c(deletedCovariateIds, categoryToDelete)
          t <- covariates$covariateId == categoryToDelete
          covariates <- covariates[ffbase::ffwhich(t, t == FALSE), ]
        }
      }
    }
  }
  delta <- Sys.time() - start
  writeLines(paste("Removing redundant covariates took", signif(delta, 3), attr(delta, "units")))
  
  metaData <- list(sql = renderedSql,
                   call = match.call(),
                   deletedCovariateIds = deletedCovariateIds)
  result <- list(covariates = covariates, covariateRef = covariateRef, metaData = metaData)
  class(result) <- "covariateData"
  return(result)
}


#' Create HDPS covariate settings
#'
#' @details
#' creates an object specifying how covariates should be contructed from data in the CDM model.
#'
#' @param useCovariateCohortIdIs1                             A boolean value (TRUE/FALSE) to determine
#'                                                            if a covariate should be contructed for
#'                                                            whether the cohort ID is 1 (currently
#'                                                            primarily used in CohortMethod).
#' @param useCovariateDemographics                            A boolean value (TRUE/FALSE) to determine
#'                                                            if demographic covariates (age in 5-yr
#'                                                            increments, gender, race, ethnicity, year
#'                                                            of index date, month of index date) will
#'                                                            be created and included in future models.
#' @param useCovariateDemographicsGender                      A boolean value (TRUE/FALSE) to determine
#'                                                            if gender should be included in the
#'                                                            model.
#' @param useCovariateDemographicsRace                        A boolean value (TRUE/FALSE) to determine
#'                                                            if race should be included in the model.
#' @param useCovariateDemographicsEthnicity                   A boolean value (TRUE/FALSE) to determine
#'                                                            if ethnicity should be included in the
#'                                                            model.
#' @param useCovariateDemographicsAge                         A boolean value (TRUE/FALSE) to determine
#'                                                            if age (in 5 year increments) should be
#'                                                            included in the model.
#' @param useCovariateDemographicsYear                        A boolean value (TRUE/FALSE) to determine
#'                                                            if calendar year should be included in
#'                                                            the model.
#' @param useCovariateDemographicsMonth                       A boolean value (TRUE/FALSE) to determine
#'                                                            if calendar month should be included in
#'                                                            the model.
#' @param useCovariateConditionOccurrence                     A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates derived from
#'                                                            CONDITION_OCCURRENCE table will be
#'                                                            created and included in future models.
#' @param useCovariate3DigitIcd9Inpatient180d                 A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates will be created and used in
#'                                                            models that look for presence/absence of
#'                                                            condition within inpatient setting in
#'                                                            180d window prior to or on cohort index
#'                                                            date. Conditions are aggregated at the
#'                                                            ICD-9 3-digit level. Only applicable if
#'                                                            useCovariateConditionOccurrence = TRUE.
#' @param useCovariate3DigitIcd9Inpatient180dMedF             Similar to
#'                                                            \code{useCovariate3DigitIcd9Inpatient180d},
#'                                                            but now only if the frequency of the
#'                                                            ICD-9 code is higher than the median.
#' @param useCovariate3DigitIcd9Inpatient180d75F              Similar to
#'                                                            \code{useCovariate3DigitIcd9Inpatient180d},
#'                                                            but now only if the frequency of the
#'                                                            ICD-9 code is higher than the 75th
#'                                                            percentile.
#' @param useCovariate3DigitIcd9Ambulatory180d                A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates will be created and used in
#'                                                            models that look for presence/absence of
#'                                                            condition within ambulatory setting in
#'                                                            180d window prior to or on cohort index
#'                                                            date. Conditions are aggregated at the
#'                                                            ICD-9 3-digit level. Only applicable if
#'                                                            useCovariateConditionOccurrence = TRUE.
#' @param useCovariate3DigitIcd9Ambulatory180dMedF            Similar to
#'                                                            \code{useCovariate3DigitIcd9Ambulatory180d},
#'                                                            but now only if the frequency of the
#'                                                            ICD-9 code is higher than the median.
#' @param useCovariate3DigitIcd9Ambulatory180d75F             Similar to
#'                                                            \code{useCovariate3DigitIcd9Ambulatory180d},
#'                                                            but now only if the frequency of the
#'                                                            ICD-9 code is higher than the 75th
#'                                                            percentile.
#' @param useCovariateDrugExposure                            A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates derived from DRUG_EXPOSURE
#'                                                            table will be created and included in
#'                                                            future models.
#' @param useCovariateIngredientExposure180d                  A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates will be created and used in
#'                                                            models that look for presence/absence of
#'                                                            drug ingredients within inpatient setting
#'                                                            in 180d window prior to or on cohort
#'                                                            index date.  Only applicable if
#'                                                            useCovariateDrugExposure = TRUE.
#' @param useCovariateIngredientExposure180dMedF              Similar to
#'                                                            \code{useCovariateIngredientExposure180d},
#'                                                            but now only if the frequency of the
#'                                                            ingredient is higher than the median.
#' @param useCovariateIngredientExposure180d75F               Similar to
#'                                                            \code{useCovariateIngredientExposure180d},
#'                                                            but now only if the frequency of the
#'                                                            ingredient is higher than the 75th
#'                                                            percentile.
#' @param useCovariateProcedureOccurrence                     A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates derived from
#'                                                            PROCEDURE_OCCURRENCE table will be
#'                                                            created and included in future models.
#' @param useCovariateProcedureOccurrenceInpatient180d        A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates will be created and used in
#'                                                            models that look for presence/absence of
#'                                                            procedures within inpatient setting in
#'                                                            180d window prior to or on cohort index
#'                                                            date.  Only applicable if
#'                                                            useCovariateProcedureOccurrence = TRUE.
#' @param useCovariateProcedureOccurrenceInpatient180dMedF    Similar to
#'                                                            \code{useCovariateProcedureOccurrenceInpatient180d},
#'                                                            but now only if the frequency of the
#'                                                            procedure code is higher than the median.
#' @param useCovariateProcedureOccurrenceInpatient180d75F     Similar to
#'                                                            \code{useCovariateProcedureOccurrenceInpatient180d},
#'                                                            but now only if the frequency of the
#'                                                            procedure code is higher than the 75th
#'                                                            percentile.
#' @param useCovariateProcedureOccurrenceAmbulatory180d       A boolean value (TRUE/FALSE) to determine
#'                                                            if covariates will be created and used in
#'                                                            models that look for presence/absence of
#'                                                            procedures within ambulatory setting in
#'                                                            180d window prior to or on cohort index
#'                                                            date.  Only applicable if
#'                                                            useCovariateProcedureOccurrence = TRUE.
#' @param useCovariateProcedureOccurrenceAmbulatory180dMedF   Similar to
#'                                                            \code{useCovariateProcedureOccurrenceAmbulatory180d},
#'                                                            but now only if the frequency of the
#'                                                            procedure code is higher than the median.
#' @param useCovariateProcedureOccurrenceAmbulatory180d75F    Similar to
#'                                                            \code{useCovariateProcedureOccurrenceAmbulatory180d},
#'                                                            but now only if the frequency of the
#'                                                            procedure code is higher than the 75th
#'                                                            percentile.
#' @param excludedCovariateConceptIds                         A list of concept IDs that should NOT be
#'                                                            used to construct covariates.
#' @param includedCovariateConceptIds                         A list of concept IDs that should be used
#'                                                            to construct covariates.
#' @param deleteCovariatesSmallCount                          A numeric value used to remove covariates
#'                                                            that occur in both cohorts fewer than
#'                                                            deleteCovariateSmallCounts time.
#'
#' @return
#' An object of type \code{hdpsCovariateSettings}, to be used in other functions.
#'
#' @export
createHdpsCovariateSettings <- function(useCovariateCohortIdIs1 = FALSE,
                                        useCovariateDemographics = TRUE,
                                        useCovariateDemographicsGender = TRUE,
                                        useCovariateDemographicsRace = TRUE,
                                        useCovariateDemographicsEthnicity = TRUE,
                                        useCovariateDemographicsAge = TRUE,
                                        useCovariateDemographicsYear = TRUE,
                                        useCovariateDemographicsMonth = TRUE,
                                        useCovariateConditionOccurrence = TRUE,
                                        useCovariate3DigitIcd9Inpatient180d = FALSE,
                                        useCovariate3DigitIcd9Inpatient180dMedF = FALSE,
                                        useCovariate3DigitIcd9Inpatient180d75F = FALSE,
                                        useCovariate3DigitIcd9Ambulatory180d = FALSE,
                                        useCovariate3DigitIcd9Ambulatory180dMedF = FALSE,
                                        useCovariate3DigitIcd9Ambulatory180d75F = FALSE,
                                        useCovariateDrugExposure = FALSE,
                                        useCovariateIngredientExposure180d = FALSE,
                                        useCovariateIngredientExposure180dMedF = FALSE,
                                        useCovariateIngredientExposure180d75F = FALSE,
                                        useCovariateProcedureOccurrence = FALSE,
                                        useCovariateProcedureOccurrenceInpatient180d = FALSE,
                                        useCovariateProcedureOccurrenceInpatient180dMedF = FALSE,
                                        useCovariateProcedureOccurrenceInpatient180d75F = FALSE,
                                        useCovariateProcedureOccurrenceAmbulatory180d = FALSE,
                                        useCovariateProcedureOccurrenceAmbulatory180dMedF = FALSE,
                                        useCovariateProcedureOccurrenceAmbulatory180d75F = FALSE,
                                        excludedCovariateConceptIds = c(),
                                        includedCovariateConceptIds = c(),
                                        deleteCovariatesSmallCount = 100) {
  # First: get the default values:
  covariateSettings <- list()
  for (name in names(formals(createHdpsCovariateSettings))) {
    covariateSettings[[name]] <- get(name)
  }
  # Next: overwrite defaults with actual values if specified:
  values <- lapply(as.list(match.call())[-1], function(x) eval(x, envir = sys.frame(-3)))
  for (name in names(values)) {
    if (name %in% names(covariateSettings))
      covariateSettings[[name]] <- values[[name]]
  }
  attr(covariateSettings, "fun") <- "getDbHdpsCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

querySql.ffdf <- function(connection, sql, errorReportFile = file.path(getwd(), "errorReport.txt")) {
  if (rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  # Calling splitSql, because this will also strip trailing semicolons (which cause Oracle to crash).
  sqlStatements <- SqlRender::splitSql(sql)
  if (length(sqlStatements) > 1)
    stop(paste("A query that returns a result can only consist of one SQL statement, but",
               length(sqlStatements),
               "statements were found"))
  tryCatch({
    result <- lowLevelQuerySql.ffdf(connection, sqlStatements[1])
    colnames(result) <- toupper(colnames(result))
    if (attr(connection, "dbms") == "impala") {
      for (colname in colnames(result)) {
        if (grepl("DATE", colname)) {
          result[[colname]] <- as.Date(result[[colname]], "%Y-%m-%d")
        }
      }
    }
    return(result)
  }, error = function(err) {
    .createErrorReport(connection@dbms, err$message, sql, errorReportFile)
  })
}


.createErrorReport <- function(dbms, message, sql, fileName) {
  report <- c("DBMS:\n", dbms, "\n\nError:\n", message, "\n\nSQL:\n", sql, "\n\n", .systemInfo())
  fileConn <- file(fileName)
  writeChar(report, fileConn, eos = NULL)
  close(fileConn)
  rlang::abort(paste("Error executing SQL:",
              message,
              paste("An error report has been created at ", fileName),
              sep = "\n"), call. = FALSE)
}

.systemInfo <- function() {
  si <- sessionInfo()
  lines <- c()
  lines <- c(lines, "R version:")
  lines <- c(lines, si$R.version$version.string)
  lines <- c(lines, "")
  lines <- c(lines, "Platform:")
  lines <- c(lines, si$R.version$platform)
  lines <- c(lines, "")
  lines <- c(lines, "Attached base packages:")
  lines <- c(lines, paste("-", si$basePkgs))
  lines <- c(lines, "")
  lines <- c(lines, "Other attached packages:")
  for (pkg in si$otherPkgs) lines <- c(lines,
                                       paste("- ", pkg$Package, " (", pkg$Version, ")", sep = ""))
  return(paste(lines, collapse = "\n"))
}

lowLevelQuerySql.ffdf <- function(connection, query = "", datesAsString = FALSE) {
  if (rJava::is.jnull(connection@jConnection))
    stop("Connection is closed")
  batchedQuery <- rJava::.jnew("org.ohdsi.databaseConnector.BatchedQuery",
                               connection@jConnection,
                               query)
  
  on.exit(rJava::.jcall(batchedQuery, "V", "clear"))
  
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (length(columnTypes) == 0)
    stop("No columns found")
  columns <- vector("list", length(columnTypes))
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    if (is.null(columns[[1]]) && rJava::.jcall(batchedQuery, "Z", "isEmpty")) {
      # Empty result set: return data frame instead because ffdf can't have zero rows
      for (i in seq.int(length(columnTypes))) {
        if (columnTypes[i] == 1) {
          columns[[i]] <- vector("numeric", length = 0)
        } else {
          columns[[i]] <- vector("character", length = 0)
        }
      }
      names(columns) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
      attr(columns, "row.names") <- c(NA_integer_, length(columns[[1]]))
      class(columns) <- "data.frame"
      return(columns)
    } else {
      for (i in seq.int(length(columnTypes))) {
        if (columnTypes[i] == 1) {
          column <- rJava::.jcall(batchedQuery,
                                  "[D",
                                  "getNumeric",
                                  as.integer(i))
          # rJava doesn't appear to be able to return NAs, so converting NaNs to NAs:
          column[is.nan(column)] <- NA
          columns[[i]] <- ffbase::ffappend(columns[[i]], column)
        } else {
          columns[[i]] <- ffbase::ffappend(columns[[i]], factor(rJava::.jcall(batchedQuery,
                                                                              "[Ljava/lang/String;",
                                                                              "getString",
                                                                              i)))
        }
      }
    }
  }
  if (!datesAsString) {
    for (i in seq.int(length(columnTypes))) {
      if (columnTypes[i] == 3) {
        columns[[i]] <- ffbase::as.Date.ff_vector(columns[[i]])
      } else  if (columnTypes[i] == 4) {
        columns[[i]] <- as.POSIXct.ff_vector(columns[[i]])
      }
    }
  }
  ffdf <- do.call(ff::ffdf, columns)
  names(ffdf) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")
  return(ffdf)
}

bySumFf <- function(values, bins) {
  ffbase::bySum(values, bins)
}


createHdPs <- function(cohortMethodData,
                     population = NULL,
                     excludeCovariateIds = c(),
                     includeCovariateIds = c(),
                     maxCohortSizeForFitting = 250000,
                     errorOnHighCorrelation = TRUE,
                     stopOnError = TRUE,
                     prior = createPrior("laplace", exclude = c(0), useCrossValidation = TRUE),
                     control = createControl(noiseLevel = "silent",
                                             cvType = "auto",
                                             seed = 1,
                                             tolerance = 2e-07,
                                             cvRepetitions = 10,
                                             startingVariance = 0.01)) {
  if (is.null(population)) {
    population <- cohortMethodData$cohorts %>%
      collect()
  }
  if (!("rowId" %in% colnames(population)))
    stop("Missing column rowId in population")
  if (!("treatment" %in% colnames(population)))
    stop("Missing column treatment in population")
  
  start <- Sys.time()
  population <- population[order(population$rowId), ]
  if (cohortMethodData$cohorts %>% count() %>% pull() == 0) {
    error <- "No subjects in population, so cannot fit model"
    sampled <- FALSE
    ref <- NULL
  } else if (cohortMethodData$covariates %>% count() %>% pull() == 0) {
    error <- "No covariate data, so cannot fit model"
    sampled <- FALSE
    ref <- NULL
  } else {
    covariates <- cohortMethodData$covariates %>%
      filter(.data$rowId %in% local(population$rowId))
    
    if (length(includeCovariateIds) != 0) {
      covariates <- covariates %>%
        filter(.data$covariateId %in% includeCovariateIds)
    }
    if (length(excludeCovariateIds) != 0) {
      covariates <- covariates %>%
        filter(!.data$covariateId %in% excludeCovariateIds)
    }
    filteredCovariateData <- Andromeda::andromeda(covariates = covariates,
                                                  covariateRef = cohortMethodData$covariateRef,
                                                  analysisRef = cohortMethodData$analysisRef)
    metaData <- attr(cohortMethodData, "metaData")
    metaData$populationSize <- nrow(population)
    attr(filteredCovariateData, "metaData") <- metaData
    class(filteredCovariateData) <- "CovariateData"
    
    covariateData <- FeatureExtraction::tidyCovariateData(filteredCovariateData)
    close(filteredCovariateData)
    on.exit(close(covariateData))
    covariates <- covariateData$covariates
    attr(population, "metaData")$deletedInfrequentCovariateIds <- attr(covariateData, "metaData")$deletedInfrequentCovariateIds
    attr(population, "metaData")$deletedRedundantCovariateIds <- attr(covariateData, "metaData")$deletedRedundantCovariateIds
    sampled <- FALSE
    if (maxCohortSizeForFitting != 0) {
      set.seed(0)
      targetRowIds <- population$rowId[population$treatment == 1]
      if (length(targetRowIds) > maxCohortSizeForFitting) {
        ParallelLogger::logInfo(paste0("Downsampling target cohort from ", length(targetRowIds), " to ", maxCohortSizeForFitting, " before fitting"))
        targetRowIds <- sample(targetRowIds, size = maxCohortSizeForFitting, replace = FALSE)
        sampled <- TRUE
      }
      comparatorRowIds <- population$rowId[population$treatment == 0]
      if (length(comparatorRowIds) > maxCohortSizeForFitting) {
        ParallelLogger::logInfo(paste0("Downsampling comparator cohort from ", length(comparatorRowIds), " to ", maxCohortSizeForFitting, " before fitting"))
        comparatorRowIds <- sample(comparatorRowIds, size = maxCohortSizeForFitting, replace = FALSE)
        sampled <- TRUE
      }
      if (sampled) {
        fullPopulation <- population
        fullCovariates <- covariates
        population <- population[population$rowId %in% c(targetRowIds, comparatorRowIds), ]
        covariates <- covariates %>%
          filter(.data$rowId %in% local(population$rowId))
      }
    }
    population <- population[order(population$rowId), ]
    outcomes <- population
    colnames(outcomes)[colnames(outcomes) == "treatment"] <- "y"
    covariateData$outcomes <- outcomes
    floatingPoint <- getOption("floatingPoint")
    if (is.null(floatingPoint)) {
      floatingPoint <- 64
    } else {
      ParallelLogger::logInfo("Cyclops using precision of ", floatingPoint)
    }
    cyclopsData <- Cyclops::convertToCyclopsData(covariateData$outcomes, covariates, modelType = "lr", quiet = TRUE, floatingPoint = floatingPoint)
    error <- NULL
    ref <- NULL
    if (errorOnHighCorrelation) {
      suspect <- Cyclops::getUnivariableCorrelation(cyclopsData, threshold = 0.5)
      suspect <- suspect[!is.na(suspect)]
      if (length(suspect) != 0) {
        covariateIds <- as.numeric(names(suspect))
        ref <- cohortMethodData$covariateRef %>%
          filter(.data$covariateId %in% covariateIds) %>%
          collect()
        ParallelLogger::logInfo("High correlation between covariate(s) and treatment detected:")
        ParallelLogger::logInfo(paste(colnames(ref), collapse = "\t"))
        for (i in 1:nrow(ref))
          ParallelLogger::logInfo(paste(ref[i, ], collapse = "\t"))
        message <- "High correlation between covariate(s) and treatment detected. Perhaps you forgot to exclude part of the exposure definition from the covariates?"
        if (stopOnError) {
          stop(message)
        } else {
          error <- message
        }
      }
    }
  }
  if (is.null(error)) {
    cyclopsFit <- tryCatch({
      Cyclops::fitCyclopsModel(cyclopsData, prior = prior, control = control)
    }, error = function(e) {
      e$message
    })
    if (is.character(cyclopsFit)) {
      if (stopOnError) {
        stop(cyclopsFit)
      } else {
        error <- cyclopsFit
      }
    } else if (cyclopsFit$return_flag != "SUCCESS") {
      if (stopOnError) {
        stop(cyclopsFit$return_flag)
      } else {
        error <- cyclopsFit$return_flag
      }
    }
  }
  if (is.null(error)) {
    error <- "OK"
    cfs <- coef(cyclopsFit)
    if (all(cfs[2:length(cfs)] == 0)) {
      warning("All coefficients (except maybe the intercept) are zero. Either the covariates are completely uninformative or completely predictive of the treatment. Did you remember to exclude the treatment variables from the covariates?")
    }
    if (sampled) {
      # Adjust intercept to non-sampled population:
      y.bar <- mean(population$treatment)
      y.odds <- y.bar/(1 - y.bar)
      y.bar.new <- mean(fullPopulation$treatment)
      y.odds.new <- y.bar.new/(1 - y.bar.new)
      delta <- log(y.odds) - log(y.odds.new)
      cfs[1] <- cfs[1] - delta  # Equation (7) in King and Zeng (2001)
      cyclopsFit$estimation$estimate[1] <- cfs[1]
      covariateData$fullOutcomes <- fullPopulation
      population <- fullPopulation
      population$propensityScore <- predict(cyclopsFit, newOutcomes = covariateData$fullOutcomes, newCovariates = fullCovariates)
    } else {
      population$propensityScore <- predict(cyclopsFit)
    }
    attr(population, "metaData")$psModelCoef <- coef(cyclopsFit)
    attr(population, "metaData")$psModelPriorVariance <- cyclopsFit$variance[1]
  } else {
    if (sampled) {
      population <- fullPopulation
    }
    population$propensityScore <- population$treatment
    attr(population, "metaData")$psError <- error
    if (!is.null(ref)) {
      attr(population, "metaData")$psHighCorrelation <- ref
    }
  }
  population <- computePreferenceScore(population)
  delta <- Sys.time() - start
  ParallelLogger::logDebug("Propensity model fitting finished with status ", error)
  ParallelLogger::logInfo("Creating propensity scores took ", signif(delta, 3), " ", attr(delta, "units"))
  return(population)
}

computePreferenceScore <- function(data, unfilteredData = NULL) {
  if (is.null(unfilteredData)) {
    proportion <- sum(data$treatment)/nrow(data)
  } else {
    proportion <- sum(unfilteredData$treatment)/nrow(unfilteredData)
  }
  propensityScore <- data$propensityScore
  propensityScore[propensityScore > 0.9999999] <- 0.9999999
  x <- exp(log(propensityScore/(1 - propensityScore)) - log(proportion/(1 - proportion)))
  data$preferenceScore <- x/(x + 1)
  return(data)
}