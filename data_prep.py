
import json
from typing import Literal
import pandas as pd
import mylogger
from data_config import keep_parent_fields
from utils.dtypes import convert_dtypes, convert_to_datetime
from utils.df_ops_base import concat_drop_parent, \
                            drop_notes_by_regex \
                      , normalize_first_element,  drop_fields
from utils.fromstr import clean_and_parse_json
from data_config import EstablishmentID_Program, nada_cols, activities_w_days
from process_odc_cols import expand_drug_info

logger = mylogger.get(__name__)

"""
  limit the dataset to only those clients who have done at least min_num_assessments
"""
def limit_min_num_assessments(df, min_num_assessments):

    g = df.groupby('SLK')
    counts =  g['SLK'].count()
    logger.debug(f"limit_min_num_assessments: counts: {counts}")
    gt2_ATOM_SLKs = counts[ counts >= min_num_assessments ].index.tolist()
    df_gt2 = df [ df['SLK'].isin(gt2_ATOM_SLKs) ]
    logger.debug(f"limit_min_num_assessments: {len(df_gt2)} clients with at least {min_num_assessments} assessments")
    return df_gt2


def limit_clients_active_inperiod(df, start_date, end_date):
  # clients_inperiod = df[ (df.AssessmentDate >=  '2022-07-01') & (df.AssessmentDate <= '2023-06-30')].SLK.unique()
  logger.debug(f"Total clients {len(df)}")
  
  clients_inperiod = df[ (df.AssessmentDate >=  start_date) & (df.AssessmentDate <= end_date)].SLK.unique()
  df_active_clients = df [ df['SLK'].isin(clients_inperiod) ]
  
  logger.debug(f"Clients in period {len(df_active_clients)}")

  return df_active_clients


def get_surveydata_expanded(df:pd.DataFrame):#, prep_type:Literal['ATOM', 'NADA', 'Matching'] ) -> pd.DataFrame: 
  # https://dschoenleber.github.io/pandas-json-performance/
  
  logger.debug("\t get_surveydata_expanded")

  df_surveydata = df['SurveyData'].apply(clean_and_parse_json)
  df_surveydata_expanded:pd.DataFrame =  pd.json_normalize(df_surveydata.tolist(), max_level=1)
  
  if keep_parent_fields:
    existing_columns_to_remove = [col for col in keep_parent_fields if col in df_surveydata_expanded.columns]
    if existing_columns_to_remove:
      df_surveydata_expanded = drop_fields(df_surveydata_expanded, keep_parent_fields)
  # df_surveydata_expanded = df_surveydata_expanded[ keep_parent_fields[prep_type] ]
  df_final  = concat_drop_parent(df, df_surveydata_expanded, drop_parent_name='SurveyData')
  return df_final


def ep_dates(raw_df:pd.DataFrame, columns:list[str])->pd.DataFrame:
  df = raw_df.copy()
  for col in columns:
      if not pd.api.types.is_integer_dtype(df[col].dtype):
        if (df[col] == '').any():
          df[col] = df[col].replace('', 0).astype(int)
        else:
          df[col] = df[col].fillna(0).astype(int)

      # Convert integer to string with zero-padding to ensure length is 8
      df[col] = df[col].astype(str).str.zfill(8)
      
      # Reformat string to match datetime format 'ddmmyyyy' -> 'dd-mm-yyyy'
      df[col] = df[col].apply(lambda x: f"{x[:2]}-{x[2:4]}-{x[4:]}")
      
      # Convert string to datetime
      df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
  return df  



def prep_dataframe_episodes(df:pd.DataFrame) -> pd.DataFrame: 
  processed_ep_df =  ep_dates(df, columns=['START_DATE', 'END_DATE'])
  processed_ep_df['Program'] = processed_ep_df['PartitionKey'].map(EstablishmentID_Program)
  
  rename_columns = {
      'SPECIFY_DRUG_OF_CONCERN': 'PDC',
      'START_DATE': 'CommencementDate',
      'END_DATE': 'EndDate',
      # 'RowKey': 'EpisodeID',      
  }  
  renamed_columns_of_interest = ['SLK','Program', 'CommencementDate', 'EndDate', 'PDC']
  processed_ep_df.rename(columns=rename_columns, inplace=True)
  processed_ep_df = processed_ep_df[renamed_columns_of_interest]
  return processed_ep_df


def prep_dataframe_matching(df:pd.DataFrame):
  logger.debug(f"prep_dataframe of length {len(df)} : ")

  df2 = get_surveydata_expanded(df.copy())
  convert_to_datetime(df2, 'AssessmentDate')
  if 'SLK' in df2.columns:
    df2.drop(columns=['SLK'], inplace=True) 
  
  df2.rename(columns={'PartitionKey': 'SLK'}, inplace=True)
  df3 = normalize_first_element(df2,'PDC')
  df4 = df3[['SLK', 'AssessmentDate', 'Program','Staff', 'RowKey', 'PDCSubstanceOrGambling']]
  df4 = df4.rename(columns={'PDCSubstanceOrGambling': 'PDC'})
  return df4


  # df5 = normalize_first_element(df4,'PDC')

# def get_val_or_none(dic, key ):
#   if dic and key in dic:
#     return dic[key]
#   return None

def get_days_or_none(dic):
  if pd.isna(dic):
    return None
  return dic.get('Days')
# get_val_or_none(dic, 'Days')


# [Yes - parenting responsibilities but children don't live with me] 
# [Yes - parenting responsibilities for children other than my own]  
# [Living with children other than my own, but no parental responsibility]       
def primary_care_under_5(x):
  # if pd.isna(x):
  if not x or not any(x):
    return None
  return 'Yes - primary caregiver: children under 5 years old' in x

def primary_care_5to15(x):
  if not x or not any(x):
    return None
  return 'Yes - primary caregiver: children 5 - 15 years old' in x


def expand_activities_info(df1:pd.DataFrame):
  new_fields_to_keep = []
  df5 = df1.copy()
 
  # df5['PaidWorkDays'] = df5['Past4WkEngagedInOtheractivities.Paid Work'].apply(get_days_or_none)
  for k, v in activities_w_days.items():
    df5[v] = df5[k].apply(get_days_or_none)

  new_fields_to_keep.extend(activities_w_days.values())

  #TODO: convert these to number
  # this fixes the NaNs
  df5['PrimaryCaregiver'] = df5['PrimaryCaregiver'].where(df5['PrimaryCaregiver'].notna(), None)

  df5['PrimaryCaregiver_0-5'] = df5['PrimaryCaregiver'].apply(primary_care_under_5)
  df5['PrimaryCaregiver_5-15']= df5['PrimaryCaregiver'].apply(primary_care_5to15)
  new_fields_to_keep.extend(['PrimaryCaregiver_0-5', 'PrimaryCaregiver_5-15'])

  return df5, new_fields_to_keep

def prep_dataframe_nada(df:pd.DataFrame):

  logger.debug(f"prep_dataframe of length {len(df)} : ")
  df2 = get_surveydata_expanded(df.copy())
 
  df4 = drop_notes_by_regex(df2) # remove *Goals notes, so do before PDC step (PDCGoals dropdown)
  # df5 = normalize_first_element(df4,'PDC') #TODO: (df,'ODC') # only takes the first ODC   

  df5, new_drug_fields = expand_drug_info(df4)


  # df5['PaidWork'] = df5['Past4WkEngagedInOtheractivities.Paid Work'].apply(lambda x: None if pd.isna(x) else x)
   
  df51, new_activity_fields = expand_activities_info(df5)
  all_cols = nada_cols + new_drug_fields + new_activity_fields
  # PrimaryCaregiver :  [Yes - primary caregiver: children 15 - 18 yea...
  
  # df6 = df5[df5.PDCSubstanceOrGambling.notna()]# removes rows without PDC
  df6 = df51[all_cols]
  # df6.loc[:,'Program'] = df6['RowKey'].str.split('_').str[0] # has to be made into category
  df7 = convert_dtypes(df6)

  
  df7.rename(columns={'PartitionKey': 'SLK'}, inplace=True)
  
  df9 = df7.sort_values(by="AssessmentDate")
 
  # df9['PDC'] = df9['PDCSubstanceOrGambling']
 
  logger.debug(f"Done Prepping df")
  return df9

def prep_dataframe(df:pd.DataFrame, prep_type: Literal['ATOM', 'NADA', 'Matching'] = 'ATOM'):
   # because Program is in SurveyData
  
  if prep_type == 'Matching':
    return prep_dataframe_matching(df)

  logger.debug(f"prep_dataframe of length {len(df)} : ")
  df2 = get_surveydata_expanded(df.copy())

  df3 = drop_fields(df2,['ODC'])
  df4 = drop_notes_by_regex(df3) # remove *Goals notes, so do before PDC step (PDCGoals dropdown)
  df5 = normalize_first_element(df4,'PDC') #TODO: (df,'ODC') # only takes the first ODC   

 
  df6 = df5[df5.PDCSubstanceOrGambling.notna()]# removes rows without PDC

  # df6.loc[:,'Program'] = df6['RowKey'].str.split('_').str[0] # has to be made into category
  df7 = convert_dtypes(df6)

  # df.PDCAgeFirstUsed[(df.PDCAgeFirstUsed.notna()) & (df.PDCAgeFirstUsed != '')].astype(int)
 # "Expected bytes, got a 'int' object", 'Conversion failed for column PDCAgeFirstUsed with type object'
  df8 = drop_fields(df7, ['PDCAgeFirstUsed',\
                           'PrimaryCaregiver','Past4WkAodRisks']) 
  # 'cannot mix list and non-list, non-null values', 
  # 'Conversion failed for column PrimaryCaregiver, Past4WkAodRisks with type object')

  if 'SLK' in df8.columns:
    df8.drop(columns=['SLK'], inplace=True) 
  
  df8.rename(columns={'PartitionKey': 'SLK'}, inplace=True)
  
  df9 = df8.sort_values(by="AssessmentDate")
 
  df9['PDC'] = df9['PDCSubstanceOrGambling']
 
  logger.debug(f"Done Prepping df")
  return df9


# slow compared to group_utils.limit_pkey_num_assessments
# # Limit Client by Number of ADOMs done
# def limit_by_num_atoms(atom, num_atoms=3, gt_or_eq='>='):
#   if gt_or_eq == '>':
#     fn = lambda x: len(x) > num_atoms
#   elif gt_or_eq == '=':
#     fn = lambda x: len(x) == num_atoms
#   else:
#     fn = lambda x: len(x) >= num_atoms
  
#   #https://stackoverflow.com/questions/17109419/pandas-filtering-pivot-table-rows-where-count-is-fewer-than-specified-value
#   return  atom[atom.groupby('PartitionKey')['PartitionKey'].transform(fn).astype('bool')].reset_index(drop=True) 






# def prep(df):
#   df = drop_notes_byregex(df) # remove *Goals notes, so do before PDC step (PDCGoals dropdown)
#   df = expand_nested_dictlist_firstelem(df,'PDC') # (df,'ODC') # only takes the first ODC   
#   convert_to_datetime(df,'AssessmentDate')
#   return df
# def define_categories1(adom):
# # categorize the data
#   # TODO : convert "Three or four times per week" -> Three or four times A week
#   cat_never_daily = ['Not at all','Less than weekly','Once or twice a week','Three or four times a week','Daily or almost daily']
#   cat_daily_never = reversed(cat_never_daily)
#   cat_notatall_extremely = ['Not at all','Slightly','Moderately','Considerably','Extremely']
  
#   cat_atsi =[     'Neither Aboriginal nor Torres Strait Islander', 
#        'Aboriginal but not Torres Strait Islander',
#        'Both Aboriginal and Torres Strait Islander',
#        'Torres Strait Islander but not Aboriginal'            
#           ]
#   # dt12 = pd.CategoricalDtype(cat_never_daily, ordered=True)
#   # dt16 = pd.CategoricalDtype(cat_daily_never, ordered=True)
#   # dt20 = pd.CategoricalDtype(cat_notatall_extremely, ordered=True)
#   dt_atsi = pd.CategoricalDtype(cat_atsi, ordered=True)

#   # convert all category columns to the type Category
#   for item in q12to15_17_18:
#     adom[item] =  pd.Series(adom[item], dtype=dt12)

#   adom[q16] =  pd.Series(adom[q16], dtype=dt16)
#   adom[q20] =  pd.Series(adom[q20], dtype=dt20)

#   # convert categorical fields to ints for calculations
#   cat_columns = adom.select_dtypes(['category']).columns
#   adom[cat_columns] = adom[cat_columns].apply(lambda x: x.cat.codes)




  #      'IndigenousStatus': CategoricalDtype([ 'Neither Aboriginal nor Torres Strait Islander', 
  #      'Aboriginal but not Torres Strait Islander',
  #      'Both Aboriginal and Torres Strait Islander',
  #      'Torres Strait Islander but not Aboriginal']),
  
  # 'ClientType' : CategoricalDtype([
  #       'ownuse', 'othersuse', 'PsychiatristReferral'
  #   ]),

  # 'PDCSubstanceOrGambling': CategoricalDtype([
  #   'Ethanol', 'Amphetamines, n.f.d.', 'Heroin', 'Methamphetamine',
  #      'Cannabinoids', 'Cannabinoids and Related Drugs, n.f.d.',
  #      'MDMA/Ecstasy', 'Amphetamines, n.f.d', 'Zolpidem',
  #      'Benzodiazepines, n.f.d.', 'Methadone',
  #      'Pharmaceutical Opioids, n.f.d.', 'Caffeine', 'Cocaine',
  #      'Amphetamine', 'Oxycodone', 'Alcohols, n.e.c.',
  #      'Peptide Hormones, Mimetics and Analogues, n.e.c.'
  # ]),
  # 'PDCGoals': CategoricalDtype([['Reduce Use', 'Maintain Abstinence', 'Cease Use',
  #      'Maintain Current level of use', 'Reduce Harms',
  #      'Maintain Recovery', 'Maintain Current Level of Use']]),
   
  # 'HowImportantIsChangeToYou': ['Critical for me. I need to change',
  #      "Really important. I'd like to change",
  #      "Not really important. I don't really care if I change or not",
  #      "Not at all. I don't want to change"]
  
  # # 'MethodOfUse'