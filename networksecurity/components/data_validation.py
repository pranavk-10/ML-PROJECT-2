from networksecurity.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file,write_yaml_file
from scipy.stats import ks_2samp
import pandas as pd
import os,sys

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def validate_no_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            required_columns = self.schema_config["columns"]
            df_columns = dataframe.columns.tolist()

            missing_columns = set(required_columns.keys()) - set(df_columns)
            extra_columns = set(df_columns) - set(required_columns.keys())

            if missing_columns or extra_columns:
                logging.warning(f"Missing columns: {missing_columns}")
                logging.warning(f"Unexpected columns: {extra_columns}")
                return False
            return True
        except Exception as e:
         raise NetworkSecurityException(e, sys)

    
    def detect_dataset_drift(self,base_df,current_df,threshold=0.05)-> bool:
        try:
            status = True
            report = {}
            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_sample=ks_2samp(d1,d2)
                if threshold<= is_sample.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False
                report.update({column:{
                    "p_value":float(is_sample.pvalue),
                    "drift_status":is_found
                }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path

            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)

            return status
        
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            train_status = self.validate_no_of_columns(dataframe=train_dataframe)
            test_status = self.validate_no_of_columns(dataframe=test_dataframe)

            if not train_status:
                raise Exception("Train dataframe does not contain all required columns.")
            if not test_status:
                raise Exception("Test dataframe does not contain all required columns.")

            numerical_columns = test_dataframe.select_dtypes(include=['number']).columns.tolist()
            logging.info(f"Numerical columns in test dataframe: {numerical_columns}")

            drift_status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)

            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path, index=False, header=True
            )

            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path, index=False, header=True
            )

            data_validation_artifact = DataValidationArtifact(
                validation_status=drift_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)