from utils.base import merge_dicts_exclude_keys
from data_config import funder_program_grouping, program_grouping

'''
 Either funder or program can be used to filter the data
'''
def get_filters(orig_filter):
  filters = {'Program': []}
  # can only look at one funder at a time
  if 'FunderName' in orig_filter and orig_filter['FunderName']:        
    prog_groupings = funder_program_grouping[orig_filter['FunderName']] # list of program groupings        
    for prog_grouping in prog_groupings:
      filters['Program'].extend(program_grouping[prog_grouping])

  elif 'Program' in orig_filter:
    filters['Program'] = orig_filter['Program']

  result_filters = merge_dicts_exclude_keys(filters, orig_filter, ['FunderName', 'Program'])
  return result_filters



def apply_filters(df, filters): # -> pd.DataFrame:
  
  if not filters or not any(filters.values()):
    return df
  
  for column, conditions in filters.items():
    if not conditions: # often the the list of conditions is empty, so skip
      continue
    if isinstance(conditions, list):  # if condition is a list, filter with isin
      df = df[df[column].isin(conditions)]
    else:
      print(f"Filter for {column} not a list")

  return df


def get_outfilename_for_filters(filters):
  if not filters or not any(filters.values()):
    return ""
  
  outfilename = ""
  for column, conditions in filters.items():
    if not conditions: # often the the list of conditions is empty, so skip
      continue
    if isinstance(conditions, list):  # if condition is a list, filter with isin
      outfilename += f"_{column[0:4]}-{'_'.join(conditions)}"
    else:
      outfilename += f"_{column[0:4]}-{conditions}" # just a string condition
      
  return outfilename