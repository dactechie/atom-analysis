# import pandas as pd



    
def get_df_forclients_with_atleast_n_surveys(df, n):
  clientSLKs = df[df['survey_rank'] == n].SLK.unique() # clients having a rank=n survey
  return df[df['SLK'].isin(clientSLKs)]

def get_nth_survey_values_for_question(df, nth, question):
  nth_surveys = df[df['survey_rank'] == nth]
  return nth_surveys[question]

# df_4 = get_df_forclients_with_atleast_n_surveys(pq_df, 4)
# first = get_nth_survey_values_for_question(df_4, 1, 'Past4WkHowOftenPhysicalHealthCausedProblems')
# first.value_counts(dropna=False)



# getSLKs_with_change_in_question(pq_df, 'Past4WkHowOftenPhysicalHealthCausedProblems', 1, 4, [4], [1,2])
def getSLKs_with_change_in_question(df, question, start_survey_num:int, end_survey_num:int, start_values:list, end_values:list):#, uses_cat_codes=True):
  
  if df[question].dtype.name == 'category':
  # if uses_cat_codes:
    # Filter the dataframe for first surveys where 'question_score' is 4
    first_surveys_score_4 = df[(df['survey_rank'] == start_survey_num) & (df[question].cat.codes.isin(start_values))]

    # Filter the dataframe for fourth surveys where 'question_score' is 1 or 2
    fourth_surveys_score_1_2 = df[(df['survey_rank'] == end_survey_num) & (df[question].cat.codes.isin(end_values))]

  else:        
    first_surveys_score_4 = df[(df['survey_rank'] == start_survey_num) & (df[question].isin(start_values))]
    fourth_surveys_score_1_2 = df[(df['survey_rank'] == end_survey_num) & (df[question].isin(end_values))]

  # Get the client identifiers from both filtered dataframes
  clients_first_surveys_score_4 = set(first_surveys_score_4['SLK'])
  clients_fourth_surveys_score_1_2 = set(fourth_surveys_score_1_2['SLK'])

  # Find the intersection of the two sets of clients
  clients_with_score_drop = clients_first_surveys_score_4 & clients_fourth_surveys_score_1_2
  return clients_with_score_drop

  # Now, 'clients_with_score_drop' is a set of clients whose scores for the question
  # dropped from 4 in the first survey to 1 or 2 in the fourth survey


# from data_config import rename_cols

# def remove_outliers(df: pd.DataFrame):
#   Q1 = df.quantile(0.25)
#   Q3 = df.quantile(0.75)
#   IQR = Q3 - Q1

#   return df[((df < (Q1 - 1.5 * IQR)) & (df > (Q3 + 1.5 * IQR))).all(axis=1)]




# def remove_bad_data(df) -> pd.DataFrame:
#   df = df [ ~ (df['Program'] == 'TEST') ] 
#   df = df[~df['Staff'].isin( ['Aftab.Jalal', 'Corrina.Trimarchi'])]
#   return df


# def prep(df: pd.DataFrame) -> pd.DataFrame:
#   df = remove_bad_data(df)
#   df = df.rename(columns=rename_cols)
#   df = df.sort_values(by="AssessmentDate")
#   return df