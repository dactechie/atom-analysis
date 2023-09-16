# from utils.environment import MyEnvironmentConfig
from azure.data.tables import  TableEntity
from .az_tables_query import SampleTablesQuery
# import mylogger
# logger = mylogger.get(__name__)
# from data_config import survey_datacols
# from utils.fromstr import get_date_from_yyyymmdd

# config = MyEnvironmentConfig()

def get_json_result(atom:TableEntity):
      #survey_data:dict = json.loads(atom.get("SurveyData",{}))
      result  = {
          "PartitionKey": atom.get("PartitionKey"),
          "RowKey" : atom.get("RowKey"),
          "Program": atom.get("Program"),
          "Staff": atom.get("Staff"),
          "SurveyName": atom.get("SurveyName"),
          "SurveyData": atom.get("SurveyData",{})
          #"SurveyData": survey_data
      }
      return result

def get_results(start_date, end_date) -> list[dict]:
    stq = SampleTablesQuery()
    assessment_date_limits = {u"lower": start_date, u"upper": end_date}
    fields = [u"PartitionKey", u"RowKey", u"Program", u"Staff", u"SurveyName", u"SurveyData"]
    name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper and IsActive eq 1 and Program ne 'TEST' and Status eq 'Complete'"

    results = [
         get_json_result(json_atom) 
         for json_atom in 
         stq.query_atoms(fields, filter_template=name_filter, query_params=assessment_date_limits)
         ]
    return results
