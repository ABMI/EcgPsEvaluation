# @file CodeToRun.R
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


library(CohortMethod)
library(DatabaseConnector)
library(rlang)
library(ff)
library(ffbase)
################## parameters
dbms <- ''
server <- ''
user <- ''
password <- ''
targetId <- 609
comparatorId <- 608
outcomeIds <- 506
cdmDatabaseSchema <- "cdmpv532.dbo"
cohortDbSchema <- "cohortdb.dbo"
cohortTable <- "CMaaV4T1"

#pathToDriver <- '/usr/lib/jvm/java-1.11.0-openjdk-amd64'
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = password)
connection <- DatabaseConnector::connect(connectionDetails)

#Create cohortmethoddata (Large-scale PS)

#covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE, useDemographicsAge = TRUE)
covariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
cohortMethodData <- getDbCohortMethodData(connectionDetails = connectionDetails,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          targetId = targetId,
                                          comparatorId = comparatorId,
                                          outcomeIds = outcomeIds,
                                          studyStartDate = "",
                                          studyEndDate = "",
                                          exposureDatabaseSchema = "cohortdb.dbo",
                                          exposureTable = "CMaaV4T1",
                                          outcomeDatabaseSchema = "cohortdb.dbo",
                                          outcomeTable = "CMaaV4T1",
                                          #cdmVersion = cdmVersion,
                                          #firstExposureOnly = TRUE,
                                          #removeDuplicateSubjects = "remove all",
                                          #restrictToCommonPeriod = FALSE,
                                          #washoutPeriod = 180,
                                          covariateSettings = covariateSettings,
                                          maxCohortSize = 50000)

population <- CohortMethod::createStudyPopulation(cohortMethodData = cohortMethodData,
                                                  population = NULL,
                                                  outcomeId = 506,
                                                  firstExposureOnly = FALSE,
                                                  restrictToCommonPeriod = FALSE,
                                                  washoutPeriod = 0,
                                                  removeDuplicateSubjects = FALSE,
                                                  removeSubjectsWithPriorOutcome = TRUE,
                                                  priorOutcomeLookback = 99999,
                                                  minDaysAtRisk = 1,
                                                  riskWindowStart = 0,
                                                  addExposureDaysToStart = NULL,
                                                  #startAnchor = "1970-01-01",
                                                  riskWindowEnd = 0,
                                                  addExposureDaysToEnd = NULL,
                                                  #endAnchor = "2099-12-31",
                                                  censorAtNewRiskWindow = FALSE)


psLS <- createPs(cohortMethodData=cohortMethodData,
                 population = population,
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
                                         startingVariance = 0.01))
################################################################################

#Create cohortmethoddata (hdPS)
hdpsCovariateSettings <- createHdpsCovariateSettings(useCovariateCohortIdIs1 = FALSE,
                                                     useCovariateDemographics = TRUE,
                                                     useCovariateDemographicsGender = TRUE,
                                                     useCovariateDemographicsRace = TRUE,
                                                     useCovariateDemographicsEthnicity = TRUE,
                                                     useCovariateDemographicsAge = TRUE,
                                                     useCovariateDemographicsYear = TRUE,
                                                     useCovariateDemographicsMonth = TRUE,
                                                     useCovariateConditionOccurrence = TRUE,
                                                     useCovariate3DigitIcd9Inpatient180d = TRUE,
                                                     useCovariate3DigitIcd9Inpatient180dMedF = FALSE,
                                                     useCovariate3DigitIcd9Inpatient180d75F = FALSE,
                                                     useCovariate3DigitIcd9Ambulatory180d = FALSE,
                                                     useCovariate3DigitIcd9Ambulatory180dMedF = FALSE,
                                                     useCovariate3DigitIcd9Ambulatory180d75F = FALSE,
                                                     useCovariateDrugExposure = TRUE,
                                                     useCovariateIngredientExposure180d = TRUE,
                                                     useCovariateIngredientExposure180dMedF = FALSE,
                                                     useCovariateIngredientExposure180d75F = FALSE,
                                                     useCovariateProcedureOccurrence = TRUE,
                                                     useCovariateProcedureOccurrenceInpatient180d = FALSE,
                                                     useCovariateProcedureOccurrenceInpatient180dMedF = FALSE,
                                                     useCovariateProcedureOccurrenceInpatient180d75F = FALSE,
                                                     useCovariateProcedureOccurrenceAmbulatory180d = FALSE,
                                                     useCovariateProcedureOccurrenceAmbulatory180dMedF = FALSE,
                                                     useCovariateProcedureOccurrenceAmbulatory180d75F = FALSE,
                                                     excludedCovariateConceptIds = c(),
                                                     includedCovariateConceptIds = c(),
                                                     deleteCovariatesSmallCount = 100)

cohortMethodData2 <- getDbCohortMethodDataHdps(connectionDetails = connectionDetails,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          targetId = targetId,
                                          comparatorId = comparatorId,
                                          outcomeIds = outcomeIds,
                                          studyStartDate = "",
                                          studyEndDate = "",
                                          exposureDatabaseSchema = "cohortdb.dbo",
                                          exposureTable = "CMaaV4T1",
                                          outcomeDatabaseSchema = "cohortdb.dbo",
                                          outcomeTable = "CMaaV4T1",
                                          #cdmVersion = cdmVersion,
                                          #firstExposureOnly = TRUE,
                                          #removeDuplicateSubjects = "remove all",
                                          #restrictToCommonPeriod = FALSE,
                                          #washoutPeriod = 180,
                                          covariateSettings = hdpsCovariateSettings,
                                          maxCohortSize = 50000,
                                          populationIds = "608, 609")

population2 <- CohortMethod::createStudyPopulation(cohortMethodData = cohortMethodData2,
                                                  population = NULL,
                                                  outcomeId = 506,
                                                  firstExposureOnly = FALSE,
                                                  restrictToCommonPeriod = FALSE,
                                                  washoutPeriod = 0,
                                                  removeDuplicateSubjects = FALSE,
                                                  removeSubjectsWithPriorOutcome = TRUE,
                                                  priorOutcomeLookback = 99999,
                                                  minDaysAtRisk = 1,
                                                  riskWindowStart = 0,
                                                  addExposureDaysToStart = NULL,
                                                  #startAnchor = "1970-01-01",
                                                  riskWindowEnd = 0,
                                                  addExposureDaysToEnd = NULL,
                                                  #endAnchor = "2099-12-31",
                                                  censorAtNewRiskWindow = FALSE)


psHdps <- createHdPs(cohortMethodData=Andromeda::andromeda(covariates = as.data.frame(cohortMethodData2$covariates), covariateRef = as.data.frame(cohortMethodData2$covariateRef),
                                                           cohorts = cohortMethodData2$cohorts, outcomes = cohortMethodData2$outcomes),
                     population = population2,
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
                                             startingVariance = 0.01))
################################################



logit <- function(p){
  log(p / (1 - p))
}

matchPsInternal <- function(propensityScores, treatment, maxRatio, caliper) {
  .Call('_CohortMethod_matchPsInternal', PACKAGE = 'CohortMethod', propensityScores, treatment, maxRatio, caliper)
}

colnames(population) <- c("treatment", "row_id", "cohort_start_date", "diff", "xml_path")
colnames(probability) <- c("ps_treatment_1", "ps_treatment_0", "row_id")


probability <- data.frame(
  probability %>% 
    group_by(row_id) %>% 
    slice(which.min(ps_treatment_1))
)

temp <- population[,-c(4,5)] %>% group_by(row_id) %>% slice(which.min(cohort_start_date))

population <- probability %>% left_join(temp, by=c("row_id" = "row_id"))
##########
#population <- population[order(population$ps_treatment_1), ]
psHdps <- psHdps[order(psHdps$propensityScore),]

#propensityScore <- population$ps_treatment_1
propensityScore <- psHdps$propensityScore

caliper <- 0.5
caliper <- caliper * sd(propensityScore)
propensityScore <- logit(propensityScore)
#caliper <- caliper * sd(propensityScore)

maxRatio <- 5

result <- matchPsInternal(propensityScore,
                          psHdps$treatment,
                          maxRatio,
                          caliper)


result <- dplyr::as_tibble(result)
psHdps$stratumId <- result$stratumId
psHdps <- psHdps[psHdps$stratumId != -1, ]


psHdps$personId <- psHdps$row_id
matchedPop <- psHdps
matchedPop$rowId <- c(1:nrow(matchedPop))

balance <- computeCovariateBalance(matchedPop, Andromeda::andromeda(covariates = as.data.frame(cohortMethodData2$covariates), covariateRef = as.data.frame(cohortMethodData2$covariateRef),
                                                                    cohorts = cohortMethodData2$cohorts, outcomes = cohortMethodData2$outcomes))
table1 <- createCmTable1(balance)
print(table1, row.names = FALSE, right = FALSE)
#plotCovariateBalanceScatterPlot(balance, showCovariateCountLabel = TRUE, showMaxLabel = TRUE, fileName = "~/study/arrhythmia/balanceScatterplot.png")
plotCovariateBalanceScatterPlot(balance, showCovariateCountLabel = TRUE, showMaxLabel = TRUE)







































getDbEcgPath

sql <- "SELECT a.*, b.cohort_start_date, datediff(day, b.cohort_start_date, a.observation_date) as datediff
        FROM (
	          SELECT person_id, observation_date, observation_datetime, observation_source_value 
	          FROM cdmpv532_abmi.dbo.observation 
	          WHERE person_id in (select subject_id from cohortdb.dbo.CMaaV4T1 where cohort_definition_id in (608,609) and observation_concept_id = 40761354)
	          ) a left join cohortdb.dbo.CMaaV4T1 b
	  on a.person_id = b.subject_id"
sql <- SqlRender::render(sql)
sql <- SqlRender::translate(sql = sql,
                            targetDialect = attr(connection, "dbms"),
                            oracleTempSchema = NULL)
EcgPath <- DatabaseConnector::querySql(connection, sql)
colnames(EcgPath) <- tolower(colnames(EcgPath))

patients <- EcgPath %>%
  group_by(person_id) %>%
  slice_min(abs(datediff)) %>%
  slice_max(observation_datetime) %>%
  distinct()

temp_array <- array(dim = c(5000, 12))
array <- array()
for (i in 1:nrow(patients)){
  print(paste0(i, "/", nrow(patients)))
  for (n in 3:14) {
    tryCatch({
      temp_list <- readMuseXml(file.path(ecgPath <- "~/data/xml", substr(patients$observation_source_value, 3, 9999))[i])
      #print(paste0("Parsing XML file ", file.path(ecgPath <- "~/data/xml", patients$xml_path)[i]))
      ecg <- as.vector(unlist(temp_list[n]))
      ecg <- filterEcg(waveforms=ecg)
      ecg <- as.vector(ecg)
      temp_array[,n-2] <- ecg
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
  }
  array <- array(c(temp_array), dim = c(i, 5000, 12))
  temp_array <- array(NA, dim = c(5000, 12))
}

#save(array, file="~/array.Rdata")
#load(file="~/array.Rdata")
rm(array)

