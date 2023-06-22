import json
import re
import pandas as pd
from azutil.az_tables_query import SampleTablesQuery, get_json_result
# import altair as alt
from data_config import survey_datacols
# from utils.fromstr import get_date_from_yyyymmdd

def get_results(start_date, end_date) -> list[dict]:
    stq = SampleTablesQuery()

    # fields = [u"PartitionKey", u"RowKey", u"SurveyData"]
    # assessment_date_limits = {u"lower": 20220701, u"upper": 20230310}
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

def get_first_of_list(pdfdictlist): # same 
    if isinstance(pdfdictlist,list):
      return pdfdictlist[0]
    return None  


# TODO: what about exlcuding Substance/Relatioship ITSP Issues, Goals Notes
def filter_cols(df:pd.DataFrame):
  orig_cols = df.columns.to_list() 
  results = survey_datacols

  for col in orig_cols:
    if re.search(".*_Score$|^Past4Wk.*",col) and \
        re.search(("^((?!Comment).)*$"),col) and \
        re.search(("^((?!Note).)*$"), col):
      results.append(col)
      
  
  return df.loc[:, results]
  # atoms_sd_cols1 = atoms_sdcols[atoms_sdcols.columns.intersection(survey_datacols)]
  # atoms_datacols2 = atoms.filter(regex=(".*_Score$|^Past4Wk.*")).filter(regex=("^((?!Comment).)*$")).filter(regex=("^((?!Note).)*$"))


def get_all_data(filename:str) -> pd.DataFrame:
  # Imported ATOM.csv
  atoms_allcols = pd.read_csv(filename)#, usecols=toplevel_cols)
  atoms_allcols.drop(['AssessmentDate', 'Program', 'Staff', 'AssessmentType'],axis=1,inplace=True)

  atoms_sdcols = pd.json_normalize(atoms_allcols['SurveyData'].apply(json.loads))   
  atoms_allcols_expanded = atoms_sdcols.join(
                   atoms_allcols
                  ).drop('SurveyData',axis=1)

  atoms_allcols_expanded['PDC'] = atoms_allcols_expanded['PDC'].apply(get_first_of_list)
  atoms_allcols_expanded = pd.concat([atoms_allcols_expanded, atoms_allcols_expanded["PDC"].apply(pd.Series)], axis=1)
  atoms_allcols_expanded.drop(columns="PDC", inplace=True)
  
  # ATOM['AssessmentDate'] = do_assessment_date(ATOM)
  return filter_cols( atoms_allcols_expanded)


# def do_viz():
#   alt.Chart(df).mark_circle().encode(
#     alt.X(alt.repeat("column"), type='quantitative'),
#     alt.Y(alt.repeat("row"), type='quantitative'),
#     color='Origin:N'
#   ).properties(
#       width=150,
#       height=150
#   ).repeat(
#       row=['Program'],
#       column=['SDS_Score', 'K10_Score', 'Past4WkQualityOfLifeScore']
#   ).interactive()

if __name__ == '__main__':
  fname = r'C:\Users\aftab.jalal\dev\atom-analysis\data\ATOM.csv'
  atomdf:pd.DataFrame = get_all_data(fname)

  print(atomdf)
  # atoms_sd_cols1 = atoms_sdcols[atoms_sdcols.columns.intersection(survey_datacols)]

  # atoms_datacols2 = atoms.filter(regex=(".*_Score$|^Past4Wk.*")).filter(regex=("^((?!Comment).)*$")).filter(regex=("^((?!Note).)*$"))

  # atoms['PDC'] = atoms['PDC'].apply(get_first_of_list)# same effect as pd.json_normalize(a['PDC'])
  # print(atoms)
# atoms.loc[:,'PDC'][0]['PDCSubstanceOrGambling']

  # g = group_filter_forstats(atoms, 'Past4WkQualityOfLifeScore', 2)
  # atoms.drop(['AssessmentDate', 'Program', 'Staff', 'AssessmentType'],axis=1,inplace=True
  #         )



  # atm_sorted = ATOMSD.sort_values(['AssessmentDate'])
  # # atm_sorted[atm_sorted['PartitionKey'] == 'ACSDM250619721'][['AssessmentDate', 'Past4WkQualityOfLifeScore']]
  # grp_persons_dateranked = atm_sorted.groupby('PartitionKey')['AssessmentDate'].rank('first')
  # grp_persons_dateranked.dropna()
  # print(grp_persons_dateranked)