
import pandas as pd
from .az_tables_query import SampleTablesQuery, get_json_result

# from data_config import survey_datacols
# from utils.fromstr import get_date_from_yyyymmdd

def get_results(start_date, end_date) -> list[dict]:
    stq = SampleTablesQuery()
    assessment_date_limits = {u"lower": start_date, u"upper": end_date}
    fields = [u"PartitionKey", u"RowKey", u"Program",  u"SurveyName", u"SurveyData"]
    name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper and IsActive eq 1 and Program ne 'TEST' and Status eq 'Complete'"

    results = [
         get_json_result(json_atom) 
         for json_atom in 
         stq.query_atoms(fields, filter_template=name_filter, query_params=assessment_date_limits)
         ]
    return results

def do_assessment_date(df:pd.DataFrame):
  # Changed AssessmentDate to dtype str
  df1 = df['AssessmentDate'].astype('str')

  # Changed AssessmentDate to dtype datetime
  result = pd.to_datetime(df1, infer_datetime_format=True, errors='coerce')
  return result