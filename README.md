# STEDI Human Balance Analytics

Project 3 of Udacity's Data Engineering with AWS Nanodegree Program

Project Overview:
Contributed to the development of a data lakehouse solution to process and curate sensor data for training a machine learning model to detect steps in real-time. This involved extracting data from various sources, including STEDI Step Trainer sensors and a mobile application, and preparing it for use by data scientists to build a machine learning model. Utilized AWS Glue, Apache Spark, and other cloud-based technologies to create a scalable and secure data pipeline.

Key Responsibilities:

Data Integration & ETL: Designed and implemented AWS Glue jobs to process and transform sensor data from STEDI Step Trainer devices and the accompanying mobile app. This involved categorizing and cleaning raw data for future querying and analysis.

Data Lakehouse Design: Developed and managed a data lakehouse architecture on AWS to store curated, high-quality sensor data. The data was made available for machine learning training, ensuring it met privacy and security standards.

Data Curation for ML Training: Curated and organized training datasets for a machine learning model that detects steps based on motion sensor readings, ensuring that only data from consenting early adopters was included to maintain privacy compliance.

Privacy & Compliance: Ensured that all data handling adhered to privacy protocols, using only data from customers who had explicitly agreed to share their sensor data for research purposes.

Technologies Used: AWS Glue, Apache Spark, AWS S3, AWS Athena, Python

Requirements

The Data Science team has done some preliminary data analysis and determined that the Accelerometer Records each match one of the Customer Records. They would like you to create 2 AWS Glue Jobs that do the following:

Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.

Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.

You need to verify your Glue job is successful and only contains Customer Records from people who agreed to share their data. Query your Glue customer_trusted table with Athena and take a screenshot of the data. Name the screenshot customer_trusted(.png,.jpeg, etc.).
Data Scientists have discovered a data quality issue with the Customer Data. The serial number should be a unique identifier for the STEDI Step Trainer they purchased. However, there was a defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of customers! Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers.

The problem is that because of this serial number bug in the fulfillment data (Landing Zone), we don’t know which customer the Step Trainer Records data belongs to.

The Data Science team would like you to write a Glue job that does the following:

Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.
Finally, you need to create two Glue Studio jobs that do the following tasks:

Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.
