# @file FilterEcg.R
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

library(signal)

filterEcg <- function(waveforms, fs = 500, cutoff_high = 0.1, cutoff_low = 20, powerline = 60, order = 5)
{
  # HZ of ECG
  #fs <- 500
  # Order of five works well with ECG signals
  #cutoff_high = 0.1
  #cutoff_low = 20
  #powerline = 60
  #order = 5
  
  # highpass
  cutoff_high <- cutoff_high
  nyq <- 0.5*fs
  normal_cutoff <- cutoff_high/nyq
  
  bf <- signal::butter(order, normal_cutoff, type=c("high"))
  b <- filter(bf, waveforms)
  
  #lowpass
  cutoff_low <- cutoff_low
  nyq <- 0.5*fs
  normal_cutoff <- cutoff_low/nyq
  
  bf <- signal::butter(order, normal_cutoff, type=c("low"))
  b <- filter(bf, b)
  
  #notch
  cutoff_notch <- powerline
  nyq <- 0.5*fs
  freq <- cutoff_notch/nyq
  
  bf <- signal::fir1(30, freq, type="stop")
  b <- filter(bf, b)
  
  return(b)
}
