


from pandas.api.types import CategoricalDtype
#from typing import TYPE_CHECKING
#if TYPE_CHECKING:
import pandas as pd

category_notatall_to_daily = CategoricalDtype(['Not at all',  'Less than weekly', 'Once or twice per week',
       'Three or four times per week', 'Daily or almost daily'], ordered=True)
predef_categories = {
    'Past4WkUseLedToProblemsWithFamilyFriend':category_notatall_to_daily,
  'Past4WkHowOftenIllegalActivities':       category_notatall_to_daily,
  'Past4WkHowOftenMentalHealthCausedProblems': category_notatall_to_daily,
  'Past4WkHowOftenPhysicalHealthCausedProblems': category_notatall_to_daily, 
}

question_list_for_categories = [
  'AssessmentType',
   'IndigenousStatus',
  'ClientType',
  'CountryOfBirth',
  'LivingArrangement',
  'PDCSubstanceOrGambling',
  'PDCGoals',  
  'Past4WkUseLedToProblemsWithFamilyFriend',
  'Past4WkHowOftenIllegalActivities',
  'Past4WkHowOftenMentalHealthCausedProblems',
  'Past4WkHowOftenPhysicalHealthCausedProblems',
  'HowImportantIsChangeToYou',
  'HaveAnySocialSupport',
  'DoYouFeelSafeWhereYouLive'

]

def define_category(df:pd.DataFrame, question:str) -> CategoricalDtype:  
  unique_list = list(df[df[question].notna()][question].unique())
  return CategoricalDtype(unique_list)
  

def define_category_for_question(df:pd.DataFrame, category_name:str):
  category_type = predef_categories.get(category_name,
                         define_category(df, category_name) )
  return category_type
  # if category_name in predef_categories:
  #   category_type =  
  # category_type = define_category(df, category_name)
  
  # df[category_name] = df[category_name].astype(category_type)


  # df[category_name] = df[category_name].astype(categories[category_name])


def define_all_categories(df:pd.DataFrame):
  category_nametypes = [(category
                          , define_category_for_question(df, category)
                        )
                        for category in question_list_for_categories]
  
      
  for category_name, category_type in category_nametypes:
    df[category_name] = df[category_name].astype(category_type)
  
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


# Limit Client by Number of ADOMs done
def limit_by_num_adoms(atom, num_atoms=3, gt_or_eq='>='):
  if gt_or_eq == '>':
    fn = lambda x: len(x) > num_atoms
  elif gt_or_eq == '=':
    fn = lambda x: len(x) == num_atoms
  else:
    fn = lambda x: len(x) >= num_atoms
  
  #https://stackoverflow.com/questions/17109419/pandas-filtering-pivot-table-rows-where-count-is-fewer-than-specified-value
  return  atom[atom.groupby('PartitionKey')['PartitionKey'].transform(fn).astype('bool')].reset_index(drop=True) 


  # checks if enough days have passed between each of the ADOM Collection-dates for a client.
def remove_duplicates_foreach_client(adom):
  # get a true/ false result for each Client ID
  res = adom.groupby('PartitionKey').apply(lambda grp: has_no_duplicates(grp['AssessmentDate'].diff()))
  # filter on 'true' i.e. no duplicates
  return adom[ adom['PartitionKey'].isin(res[res.values].index) ]

# checks if none the time-deltas(in days) in a set of passed-in values, were within 21 days 
def has_no_duplicates(adiff):
  return not any(d.days < 21 for d in adiff)  


def convert_dtypes(adom):
  adom['AssessmentDate'] = pd.to_datetime(adom['AssessmentDate'],format='%d/%m/%Y',dayfirst=True)
  adom = adom.fillna(0)

  adom = adom.astype(int,errors='ignore')
  how_many_cols = adom.filter(regex = 'ow many').columns  # numeric columns
  print(how_many_cols)
  for a in how_many_cols:
    adom[a] = pd.to_numeric(pd.Series(adom[str(a)]), errors='ignore')  
    if str(adom[a].dtype) == "float64" or adom[a].dtype == 'O':
      #print (f" a :{a}")
      adom[a] = adom[a].astype(int,errors='ignore')





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