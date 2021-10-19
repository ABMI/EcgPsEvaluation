# @file ReadMuseXml.R
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


library(dplyr); library(xml2); library(stringr); library(base64enc)

readMuseXmlEcgInfo <- function (ecgPath) 
{
  xml_file <- xml2::read_xml(ecgPath)
  acquisition_time <- xml_file %>% xml2::xml_find_first("/RestingECG/TestDemographics/AcquisitionTime") %>% 
    xml2::xml_contents() %>% xml2::xml_text()
  acquisition_date <- xml_file %>% xml2::xml_find_first("/RestingECG/TestDemographics/AcquisitionDate") %>% 
    xml2::xml_contents() %>% xml2::xml_text()
  acquisition_date_time <- as.POSIXct(paste(acquisition_date, 
                                            acquisition_time, sep = " "), format = "%m-%d-%Y %H:%M:%S")
  demographics <- xml_file %>% xml2::xml_find_all("/RestingECG/PatientDemographics") %>% 
    xml2::xml_contents()
  ecg_measurements <- xml_file %>% xml2::xml_find_all("/RestingECG/RestingECGMeasurements") %>% 
    xml2::xml_contents()
  demographics_column_names <- xml2::xml_name(demographics)
  ecg_column_names <- xml2::xml_name(ecg_measurements)
  demographics <- demographics %>% xml2::xml_text() %>% unlist() %>% 
    t() %>% data.frame(stringsAsFactors = FALSE)
  colnames(demographics) <- demographics_column_names
  ecg_measurements <- ecg_measurements %>% xml2::xml_text() %>% 
    unlist() %>% t() %>% data.frame(stringsAsFactors = FALSE)
  colnames(ecg_measurements) <- ecg_column_names
  clean_dx_statement <- function(data) {
    data <- data %>% xml2::xml_text() %>% stringr::str_replace_all(., 
                                                                   stringr::regex(("(userinsert)"), ignore_case = TRUE), 
                                                                   "") %>% stringr::str_split("ENDSLINE") %>% unlist() %>% 
      stringr::word(1, sep = "\\.") %>% stringr::str_split(",") %>% 
      unlist() %>% subset(stringr::str_detect(., stringr::regex(("(absent|\\bno\\b|\\bsuggests?\\b|\\bprobabl(e|y)\\b|\\bpossible\\b|\\brecommend\\b|\\bconsider\\b|\\bindicated\\b|resting)"), 
                                                                ignore_case = TRUE)) == FALSE) %>% stringr::str_c(collapse = ", ") %>% 
      tolower()
    return(data)
  }
  diagnosis <- xml_file %>% xml2::xml_find_all("/RestingECG/Diagnosis") %>% 
    clean_dx_statement()
  original_diagnosis <- xml_file %>% xml2::xml_find_all("/RestingECG/OriginalDiagnosis") %>% 
    clean_dx_statement()
  ECGInfo <- cbind(demographics, acquisition_date_time, 
                       ecg_measurements, diagnosis, original_diagnosis, as.data.frame(ecgPath, 
                                                                                      stringsAsFactors = FALSE), stringsAsFactors = FALSE)
  return(ECGInfo)
}


# Reading ECG information, waveformdata from XML-based MUSE system
readMuseXml <- function (ecgPath){
  
  xml_file <- xml2::read_xml(ecgPath)
  
  temp_list <- list()
  temp_df <- data.frame()
  
  temp_list$ECGInfo <- readMuseXmlEcgInfo(ecgPath)
  
  waveform_nodes <- xml2::xml_find_all(xml_file, "/RestingECG/Waveform/LeadData")
  for (x in seq_along(waveform_nodes)){
    temp_row <- waveform_nodes[x] %>% xml2::xml_contents() %>% xml2::xml_text() %>% t() %>% data.frame(stringsAsFactors = FALSE)
    colnames(temp_row) <- xml2::xml_name(xml2::xml_children(waveform_nodes[x]))
    # if (temp_row$LeadSampleCountTotal != 5000) {
    #   next
    # }
    temp_df <- rbind(temp_df, temp_row)
  }
  
  # if (nrow(temp_df) != 8) {
  #   stop(paste("Can not found 8 lead waveform in this xml file"))
  # }
  
  temp_df <- temp_df %>% 
    dplyr::filter(LeadByteCountTotal == as.numeric(temp_df$LeadByteCountTotal[which.max(temp_df$LeadByteCountTotal)]))
  
  temp_list$WaveFormInfo <- select(temp_df, -c(WaveFormData))
  temp_list$I <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="I",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable (Hz and LeadAmplitudeUnitsPerBit)
  temp_list$II <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="II",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable
  temp_list$V1 <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="V1",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable
  temp_list$V2 <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="V2",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable
  temp_list$V3 <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="V3",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable
  temp_list$V4 <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="V4",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable
  temp_list$V5 <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="V5",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable
  temp_list$V6 <- readBin(base64enc::base64decode(temp_df[temp_df$LeadID=="V6",]$WaveFormData), "integer", size = 2, n=5000)*4.88 # Should change constant to variable
  # II - I = IIIreadBin
  temp_list$III <- temp_list$II - temp_list$I
  # aVR = -(I + II)/2
  temp_list$aVR <- -(temp_list$I + temp_list$II)/2
  # aVL = I - II/2
  temp_list$aVL <- (temp_list$I - temp_list$II)/2
  # aVF = II - I/2
  temp_list$aVF <- (temp_list$II - temp_list$I)/2
  
  return(temp_list)
}
