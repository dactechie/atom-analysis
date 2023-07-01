
import json
import pandas as pd

from data_config import keep_parent_fields
from utils.dtypes import convert_dtypes
from utils.df_ops import concat_drop_parent, \
  drop_notes_by_regex, normalize_first_element,  drop_fields


"""
  limit the dataset to only those clients who have done at least min_num_assessments
"""
def limit_min_num_assessments(df, min_num_assessments):

    g = df.groupby('SLK')
    counts =  g['SLK'].count()
    gt2_ATOM_SLKs = counts[ counts >= min_num_assessments ].index.tolist()
    df_gt2 = df [ df['SLK'].isin(gt2_ATOM_SLKs) ]

    return df_gt2


def limit_clients_active_inperiod(df, start_date, end_date):
  # clients_inperiod = df[ (df.AssessmentDate >=  '2022-07-01') & (df.AssessmentDate <= '2023-06-30')].SLK.unique()
  clients_inperiod = df[ (df.AssessmentDate >=  start_date) & (df.AssessmentDate <= end_date)].SLK.unique()
  df_active_clients = df [ df['SLK'].isin(clients_inperiod) ]
  return df_active_clients

def get_surveydata_expanded(df:pd.DataFrame) -> pd.DataFrame:
  # https://dschoenleber.github.io/pandas-json-performance/
  df_surveydata = df['SurveyData'].apply(json.loads)
  df_surveydata_expanded:pd.DataFrame =  pd.json_normalize(df_surveydata.tolist(), max_level=1)
  
  df_surveydata_expanded = drop_fields(df_surveydata_expanded,keep_parent_fields)
  df_final  = concat_drop_parent(df, df_surveydata_expanded, 'SurveyData')
  return df_final


def prep_dataframe(df:pd.DataFrame):
   # because Program is in SurveyData
 
  df2 = get_surveydata_expanded(df.copy())
  df3 = drop_fields(df2,['ODC'])
  
  # df4 = prep(df3) # removes rows without PDC

  df4 = drop_notes_by_regex(df3) # remove *Goals notes, so do before PDC step (PDCGoals dropdown)
  df5 = normalize_first_element(df4,'PDC') #TODO: (df,'ODC') # only takes the first ODC   
  
  df6 = df5[df5.PDCSubstanceOrGambling.notna()]

  df6.loc[:,'Program'] = df6['RowKey'].str.split('_').str[0] # has to be made into category
  df7 = convert_dtypes(df6)

  # df.PDCAgeFirstUsed[(df.PDCAgeFirstUsed.notna()) & (df.PDCAgeFirstUsed != '')].astype(int)
 # "Expected bytes, got a 'int' object", 'Conversion failed for column PDCAgeFirstUsed with type object'
  df8 = drop_fields(df7, ['PDCAgeFirstUsed',\
                           'PrimaryCaregiver','Past4WkAodRisks']) 
  # 'cannot mix list and non-list, non-null values', 
  # 'Conversion failed for column PrimaryCaregiver, Past4WkAodRisks with type object')

  df8.drop(columns=['SLK'], inplace=True)
  df8.rename(columns={'PartitionKey': 'SLK'}, inplace=True)
  df9 = df8.sort_values(by="AssessmentDate")
  
  
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