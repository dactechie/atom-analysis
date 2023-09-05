import os
# from typing import TypedDict
from dotenv import load_dotenv

# envcondig = TypedDict('EnvironmentConfig', {"access_key": str, "endpoint_suffix": str, "account_name": str, "connection_string": str})

current_env =   os.getenv("APP_ENVIRONMENT", "prod")

class EnvironmentConfig:
    _instance = None
    env:str
    connection_string:str

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EnvironmentConfig, cls).__new__(cls)
            # cls._instance.env = 'local'
        return cls._instance
  
    @classmethod
    def setup(cls, env:str):
        cls.env = env
        env_file = f'.env.{cls.env}'
        if not os.path.isfile(env_file):
            raise Exception(f"Environment file {env_file} not found.")
            
        load_dotenv(env_file)
        
        # access_key = os.getenv("TABLES_PRIMARY_STORAGE_ACCOUNT_KEY")
        # endpoint_suffix = os.getenv("TABLES_STORAGE_ENDPOINT_SUFFIX")
        # account_name = os.getenv("TABLES_STORAGE_ACCOUNT_NAME")
        cls.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "BLANK_CONSTRING")
        # if not cls.connection_string or cls.connection_string == "BLANK_CONSTRING":
        #   defaultEndpointsProtocol = os.getenv("DefaultEndpointsProtocol")
        #   cls.connection_string = f"DefaultEndpointsProtocol={defaultEndpointsProtocol};AccountName={account_name};AccountKey={access_key};EndpointSuffix={endpoint_suffix}"
        
