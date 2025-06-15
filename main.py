from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig

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

        # Step 5: Output the result
        print(dataingestionartifact)

    except Exception as e:
        raise NetworkSecurityException(e, sys)
