
''' 
  The Survey JSON has various types of data : text/string, dates, numeric, number-ranges.
  The data types are defined in data_config.py and this module uses that information to 
  convert the data to the correct type.
  
'''
import pandas as pd
from pandas.api.types import CategoricalDtype
from data_config import predef_categories,\
    question_list_for_categories, data_types, option_variants #, remove_if_under_threshold

from utils.fromstr import range_average


    # - pandas.Categorical is used to create a variable that holds categorical data.

    # - pandas.api.types.CategoricalDtype is a type useful for specifying the categories and 
    # order when creating a pandas.Categorical variable, 
    # or when converting a pandas Series to categorical.
###############################################

def define_category_with_uniqs(df:pd.DataFrame, question:str) -> CategoricalDtype:  
  unique_list = list(df[df[question].notna()][question].unique())
  return CategoricalDtype(unique_list)
  

def define_category_for_question(df:pd.DataFrame, field:str):
  # if not a predefined category, define it using the unique values
  category_type = predef_categories.get(field,
                         define_category_with_uniqs(df, field) )
  return category_type


def define_all_categories(df:pd.DataFrame):
  category_nametypes = [(field
                          , define_category_for_question(df, field)
                        )
                        for field in question_list_for_categories]
  
  df1 = df.copy()  
  # Field elements must be 2- or 3-tuples, got ''Yes - Completely safe''
  for category_name, category_type in category_nametypes:
    #if category_type is not None and category_type..check type:
    # (isinstance(series.dtype, pd.api.types.CategoricalDtype):):
    df1[category_name] = df[category_name].astype(category_type)
  
  return df1
  
###############################################

def convert_to_datetime(df, column_names:list[str]|str):
  df [column_names] = pd.to_datetime(df[column_names])

"""
      # fix_variants       
      # d1 = df.loc[df['PDCMethodOfUse'] =='Ingests'].copy()
      # d1.loc[:,'PDCMethodOfUse'] = 'Ingest'
      # df.update(d1)

"""
def fix_variants (df1):
  df = df1.copy()
  for field, variant_dict in option_variants.items():  # type: ignore #PDCMethodOfUse
    for variant, original in variant_dict.items(): # type: ignore
      print(f'fixing {field} {variant} to {original}')
      d1 = df.loc[df[field] == variant].copy()
      d1.loc[:,field] = original
      df.update(d1)
  return df


  # convert numeric types
def fix_numerics(df1):

  df = df1.copy()
  
  numeric_fields = [k for k, v in data_types.items() if v == 'numeric']  
  df[numeric_fields] = df[numeric_fields].apply(pd.to_numeric, errors='coerce') # ignore ?

  range_fields = [k for k, v in data_types.items() if v == 'range']  
  df[range_fields] = df[range_fields].applymap(range_average)

  return df


def convert_dtypes(df1):
  df = df1.copy()
  
  convert_to_datetime(df,'AssessmentDate') # TODO : DOB
  
  df1 = fix_variants(df) # Smokes -> Smoke
  df2 = fix_numerics(df1)
  df3 = define_all_categories(df2)

  return df3
  