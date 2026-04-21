-- Training with BOOSTED TREE REGRESSOR
CREATE OR REPLACE MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_model`
  OPTIONS (model_type="BOOSTED_TREE_REGRESSOR", enable_global_explain=TRUE, input_label_cols=["total_amount"])
AS 
SELECT * FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_train_data` LIMIT 10000;

SELECT COUNT(*) FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_test_data`;

-- Evaluate the trained model with the test data
SELECT * FROM 
ML.EVALUATE(MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_model`, 
(SELECT * FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_test_data`));

-- Example for making predictions from the model
SELECT * FROM
ML.PREDICT (MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_model`, 
(SELECT * FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_test_data` LIMIT 10));

-- Query the model's global explanations
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_model`);

-- Training with RANDOM FOREST REGRESSOR
CREATE OR REPLACE MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_rf`
  OPTIONS (model_type="RANDOM_FOREST_REGRESSOR", enable_global_explain=TRUE, input_label_cols=["total_amount"])
AS 
SELECT * FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_train_data` LIMIT 1000000;

SELECT * FROM 
ML.EVALUATE(MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_rf`, 
(SELECT * FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_test_data`));

-- Training with DNN REGRESSOR
CREATE OR REPLACE MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_dnn`
  OPTIONS (model_type="DNN_REGRESSOR", enable_global_explain=TRUE, input_label_cols=["total_amount"])
AS 
SELECT * FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_train_data`;

-- Training with AUTOML REGRESSOR
CREATE OR REPLACE MODEL `smiling-rhythm-486213-b9.ml_dataset.yellow_trips_automl`
  OPTIONS (model_type="AUTOML_REGRESSOR", enable_global_explain=TRUE, input_label_cols=["total_amount"])
AS 
SELECT * FROM `smiling-rhythm-486213-b9.ml_dataset.preprocessed_train_data`;
