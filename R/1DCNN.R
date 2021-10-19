# @file 1DCNN.R
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

## TEMPORAL 1D CNN MODEL
model <- keras_model_sequential() %>%
  keras::layer_conv_1d(filters = 12, kernel_size = 16, padding = "same", 
                      use_bias = FALSE, kernel_initializer = 'he_normal', input_shape = c(5000, 12)) %>% 
  keras::layer_batch_normalization() %>%
  keras::layer_activation_relu() %>%
  #keras::layer_max_pooling_1d(64, strides = 32, padding = "same") %>%
  keras::layer_conv_1d(filters = 128, kernel_size = kernel_size, padding = "same",
                      use_bias = FALSE, kernel_initializer = kernel_initializer) %>%
  keras::layer_activation_relu() %>%
  keras::layer_batch_normalization() %>%
# 2nd layer
   keras::layer_conv_1d(filters = 16, kernel_size = kernel_size, padding = "same",
                           +                        use_bias = FALSE, kernel_initializer = kernel_initializer) %>%
  keras::layer_batch_normalization() %>%
  keras::layer_activation_relu() 
model %>%
  layer_flatten() %>% 
  layer_dense(units = 64, activation = "relu") %>% 
  layer_dense(units = 2, activation = "softmax") 
model %>% compile( 
  loss = "sparse_categorical_crossentropy", 
  optimizer = "sgd", 
  metrics = "accuracy" )
model %>% fit( x = x_train, y = y_train, epochs = 30, validation_split = 0.2, verbose = 2 )


