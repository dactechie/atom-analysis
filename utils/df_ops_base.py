
import pandas as pd

def drop_fields(df:pd.DataFrame, fieldnames:list or str or tuple):
  df2 = df.drop(fieldnames, axis=1)
  return df2


def concat_drop_parent(df, df2 ,parent_name:str) -> pd.DataFrame:
   return pd.concat([df.drop(parent_name, axis=1), df2], axis=1)




#df.loc[:,~df.columns.str.contains('num')]
def drop_notes_by_regex(df):
  # 'OtherAddictiveBehaviours.Other (detail in notes below)'
  df2 = df.loc[:,~df.columns.str.contains('Comment|Note|ITSP', case=False)]

  # df2 = df.loc[:,~df.columns.str.contains('Comment', regex=False)  # & ~df.columns.str.contains('Note', regex=False) 
  #            ]
  return df2


# get PDC - it is the first/only list item in the PDC list
def normalize_first_element (l1:pd.DataFrame, dict_key:str):#, support:Optional[dict]):
  
  masked_rows=  l1[(dict_key in l1) and l1[dict_key].apply(lambda x: isinstance(x,list) and len(x) > 0  )]
  
  # first dict of the list of dicts
  pdcs_df = masked_rows[dict_key].map(lambda x: x[0])
  normd_pdc:pd.DataFrame = pd.json_normalize(pdcs_df.to_list())  # index lost
  
  # l1.loc[7537,'PDC'] == masked_rows['PDC'][7537] == normd_pdc.loc[7317,:]
  l2 = masked_rows.reset_index(drop=True)
  result = concat_drop_parent(l2, normd_pdc, dict_key)
  return result