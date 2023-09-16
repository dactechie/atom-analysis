# coding: utf-8


"""
FILE: sample_query_table.py
DESCRIPTION:
    These samples demonstrate the following: querying a table for entities.
USAGE:
    python sample_query_table.py
    Set the environment variables with your own values before running the sample:
    1) AZURE_STORAGE_CONNECTION_STRING - the connection string to your storage account
"""
from azure.data.tables import TableClient
from azure.core.exceptions import HttpResponseError
from utils.environment import MyEnvironmentConfig
import mylogger

logger = mylogger.get(__name__)

class SampleTablesQuery(object):

    def __init__(self):
      config = MyEnvironmentConfig()
    
      self.connection_string = config.connection_string

      self.table_name = config.survey_table_name
      logger.info(f"SampleTablesQuery initialised with connection_string: {self.connection_string}")

        
    def query_atoms(self, select_fields:list[str], filter_template:str, query_params:dict):    
        """
          parameters = {u"lower": 20211201, u"upper": 20220110}
          name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper"
        """
        with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
            try:
                queried_entities = table_client.query_entities(
                    query_filter=filter_template, select=select_fields, parameters=query_params
                )

                for entity_chosen in queried_entities:
                    yield entity_chosen

            except HttpResponseError as e:
                print(e.message)
      


# def build_query_components(filter):
#   name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper"

# def main() -> list[dict]:
#     stq = SampleTablesQuery()

#     fields = [u"PartitionKey", u"RowKey", u"Progam",  u"SurveyName", u"SurveyData"]
#     assessment_date_limits = {u"lower": 20211201, u"upper": 20220110}
    
#     name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper and IsActive eq 1 and Progam ne 'TEST' and Status eq 'Complete'"
#     results = [
#          get_json_result(json_atom) 
#          for json_atom in 
#          stq.query_atoms(fields, filter_template=name_filter, query_params=assessment_date_limits)
#          ]
#     return results


# if __name__ == "__main__":
#   results = main()
#   print(results)
    
        # stq.insert_random_entities()
        # stq.sample_query_entities()
        # stq.sample_query_entities_multiple_params()
        # stq.sample_query_entities_values()
    
        # stq.clean_up()


   # def sample_query_entities(self):
    #     from azure.data.tables import TableClient
    #     from azure.core.exceptions import HttpResponseError

    #     print("Entities with name: marker")
    #     # [START query_entities]
    #     with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #         try:
    #             parameters = {"name": "marker"}
    #             name_filter = "Name eq @name"
    #             queried_entities = table_client.query_entities(
    #                 query_filter=name_filter, select=["Brand", "Color"], parameters=parameters
    #             )

    #             for entity_chosen in queried_entities:
    #                 print(entity_chosen)

    #         except HttpResponseError as e:
    #             print(e.message)
    #     # [END query_entities]

    # def sample_query_entities_multiple_params(self):
    #     from azure.data.tables import TableClient
    #     from azure.core.exceptions import HttpResponseError

    #     print("Entities with name: marker and brand: Crayola")
    #     # [START query_entities]
    #     with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #         try:
    #             parameters = {"name": "marker", "brand": "Crayola"}
    #             name_filter = "Name eq @name and Brand eq @brand"
    #             queried_entities = table_client.query_entities(
    #                 query_filter=name_filter, select=["Brand", "Color"], parameters=parameters
    #             )

    #             for entity_chosen in queried_entities:
    #                 print(entity_chosen)

    #         except HttpResponseError as e:
    #             print(e.message)
    #     # [END query_entities]

    # def sample_query_entities_values(self):
    #     from azure.data.tables import TableClient
    #     from azure.core.exceptions import HttpResponseError

    #     print("Entities with 25 < Value < 50")
    #     # [START query_entities]
    #     with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #         try:
    #             parameters = {"lower": 25, "upper": 50}
    #             name_filter = "Value gt @lower and Value lt @upper"
    #             queried_entities = table_client.query_entities(
    #                 query_filter=name_filter, select=["Value"], parameters=parameters
    #             )

    #             for entity_chosen in queried_entities:
    #                 print(entity_chosen)

    #         except HttpResponseError as e:
    #             print(e.message)