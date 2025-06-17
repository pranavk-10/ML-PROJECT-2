from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig, DataValidationConfig

import sys

if __name__ == "__main__":
    try:
        logging.info("Starting the data ingestion pipeline...")

        # Step 1: Create the Training Pipeline Config
        trainingpipelineconfig = TrainingPipelineConfig()

        # Step 2: Create the Data Ingestion Config
        data_ingestion_config = DataIngestionConfig(trainingpipelineconfig)

        # Step 3: Initialize Data Ingestion Component
        data_ingestion = DataIngestion(data_ingestion_config)

        # Step 4: Run the Data Ingestion process
        dataingestionartifact = data_ingestion.initiate_data_ingestion()
        print(dataingestionartifact)
        logging.info("Data Initiation is completed")
        data_validation_config = DataValidationConfig(trainingpipelineconfig)
        data_validation=DataValidation(dataingestionartifact,data_validation_config)
        logging.info("initiate the Data Validation process")
        data_validation_artifact=data_validation.initiate_data_validation()
        logging.info("Data Validation is completed")
        print(data_validation_artifact)

    except Exception as e:
        raise NetworkSecurityException(e, sys)
