# STEDI Human Balance Analytics

A Udacity Data Lakehouse project

Step 1: Upload the contents of `stedihumanbalanceanalyticsdata.zip` to your S3 bucket. Sample path: `s3://your-bucket/stedi-data`

step_trainer_trusted table stores user email address to be used as a join field with the user field of accelerometer_trusted table.

machine_learning_curated table was created by joining:

1. The email field from the step_trainer_trusted table and the user field from the accelerometer_trusted table.
2. The sensorreadingtime field from the step_trainer_trusted table and the timestamp field from the accelerometer_trusted table.
