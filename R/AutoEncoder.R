# @file AutoEncoder.R
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


library(dplyr)
epochNum = 500
targetDim = 100
learningRate = 1e-3#1e-2
##preprocess
dim(array)
max(array) 
min(array) 
outcomeWeight <- 1/(sum(array)/length(array))
outcomeWeight 
##Define the encoder and decoder

input_layer <- 
  keras::layer_input(shape = c(dim(array)[2], dim(array)[3]))
#keras::layer_input(shape = c(5000, 12)) 


lassoRegularizer <- keras::regularizer_l1(l = 1e-4)
ridgeRegularizer <- keras::regularizer_l2(l = 0.01)
elasticRegularizer <- keras::regularizer_l1_l2(l1 = 0.01, l2 = 0.01)

K <- keras::backend()

weighted_mse <- function(y_true, y_pred){
  keras::k_mean(keras::k_abs(y_true - y_pred)*keras::k_max(y_true*outcomeWeight, 1))
}

weighted_crossentropy <- function(y_true, y_pred){
  keras::k_binary_crossentropy(y_true, y_pred)*keras::k_max( y_true*outcomeWeight, 1)
}

metric_weighted_mse <- keras::custom_metric("weighted_mse", function(y_true, y_pred) {
  weighted_mse(y_true, y_pred)
})

metric_f1 <- function (y_true,y_pred) {
  y_pred <- keras::k_round(y_pred)
  precision <- keras::k_sum(y_pred*y_true)/(keras::k_sum(y_pred)+keras::k_epsilon())
  recall    <- keras::k_sum(y_pred*y_true)/(keras::k_sum(y_true)+keras::k_epsilon())
  (2*precision*recall)/(precision+recall+keras::k_epsilon())
} 


encoder <-
  input_layer %>%
  keras::layer_dense(units = targetDim, activity_regularizer = lassoRegularizer,activation = "sigmoid") 

decoder <- 
  encoder %>% 
  #keras::layer_dense(units = c(dim(array)[3]), activation = "relu") #the original dimension
  keras::layer_dense(units = c(1), activation = "relu") #the original dimension

##compile and train the autoencoder
autoencoder_model <- keras::keras_model(inputs = input_layer, outputs = decoder)

autoencoder_model %>% keras::compile(
  loss=weighted_crossentropy,#'mean_squared_error',#weighted_mse
  optimizer= keras::optimizer_adam(lr = learningRate),#lr=1e-2),
  metrics = c("binary_crossentropy", "binary_accuracy")
)

earlyStopping = keras::callback_early_stopping(monitor = "val_loss", 
                                               patience = 40,
                                               mode="auto",
                                               min_delta = 0)
reduceLr = keras::callback_reduce_lr_on_plateau(monitor="val_loss", factor =0.1, 
                                                patience = 15,mode = "auto", min_delta = 1e-5, cooldown = 0, min_lr = 0)
#summary(autoencoder_model)


##train onto itself
history2 <-
  autoencoder_model %>%
  keras::fit(array,
             array,
             epochs = epochNum,
             shuffle = TRUE,
             validation_split = 0.1,
             callbacks = list(earlyStopping, reduceLr)
  )

exportFolder <- "~/"

autoencoder_model %>% keras::save_model_hdf5(file.path(exportFolder,"autoencoder_model2.h5"))
autoencoder_model %>% keras::save_model_weights_hdf5(file.path(exportFolder,"autoencoder_model_weights2.h5"),overwrite = TRUE)


####Load the weights####
encoder_model <- keras::keras_model(inputs = input_layer, outputs = encoder)
encoder_model %>% keras::load_model_weights_hdf5(file.path(exportFolder,"autoencoder_model_weights2.h5"), skip_mismatch = TRUE, by_name = TRUE)
encoder_model %>% keras::compile(
  loss= weighted_crossentropy,#'mean_squared_error',#weighted_mse,#'mean_squared_error',#weighted_mse
  optimizer= keras::optimizer_adam(lr=learningRate),
  metrics = c("binary_crossentropy", "accuracy",metric_f1)
)


encodedData <- autoencoder_model %>% 
  keras::predict_on_batch (array)

reducedDim <- encoder_model %>% 
  keras::predict_on_batch (array)

#MSE
mean((as.matrix(array) -encodedData)^2) 
#Absolute difference mean
mean(abs(as.matrix(array) -encodedData)) 

# #MSE of non-zero values
# index <- which(as.matrix(data[1:10000,])==1)
# mean((as.matrix(data[1:10000,])[index] -encodedData[index])^2) 
# #Absolute difference mean  of non-zero values
# mean(abs(as.matrix(data[1:10000,])[index] -encodedData[index])) 


# ####AUROC
# pred <- ROCR::prediction(c(encodedData), c(as.matrix(data[1:10000,])))
# ROCR::performance(pred,"auc")@y.values[[1]] # 0.8046417 / 0.8065142 /  0.8190414
# 
# #perfPrc <- ROCR::performance(pred,"prec","rec")
# 
# ####Sensitivity & Specificity
# perfRoc <- ROCR::performance(pred,"tpr","fpr")
# opt.cut = function(perf, pred){
#   cut.ind = mapply(FUN=function(x, y, p){
#     d = (x - 0)^2 + (y-1)^2
#     ind = which(d == min(d))
#     c(sensitivity = y[[ind]], specificity = 1-x[[ind]], 
#       cutoff = p[[ind]])
#   }, perf@x.values, perf@y.values, pred@cutoffs)
#   return(cut.ind)
# }
# (opt.cut(perfRoc, pred)*100)[1] #sensitivity #62.01026 / 61.57023 / 64.55205
# (opt.cut(perfRoc, pred)*100)[2] #specificity #97.64553 / 99.3522 / 98.27794
# 
# ####AUPRC
# prcCurv <- PRROC::pr.curve(scores.class0 = c(encodedData), weights.class0=c(as.matrix(data[1:10000,])),curve=TRUE)
# #plot(prcCurv)
# prcCurv$auc.integral #0.6036849 / 0.638942 /  0.6455071