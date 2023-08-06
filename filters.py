import mylogger
from utils.base import merge_dicts_exclude_keys
from data_config import funder_program_grouping, program_grouping

logger = mylogger.get(__name__)

'''
 funder and/or program can be used to filter the data
'''
def get_filters(orig_filter:dict, exclude_fields=[]) -> dict:
  logger.debug(f"get_filters - orig_filter: {orig_filter}")

  filters = {'Program': []}
  # can only look at one funder at a time
  if 'FunderName' in orig_filter and orig_filter['FunderName']:        
    prog_groupings = funder_program_grouping[orig_filter['FunderName']] # list of program groupings        
    for prog_grouping in prog_groupings:
      filters['Program'].extend(program_grouping[prog_grouping])

  if 'Program' in orig_filter:
    filters['Program'].extend(orig_filter['Program'])

  result_filters = merge_dicts_exclude_keys(filters, orig_filter, exclude_fields)
  return result_filters



def apply_filters(df, filters): # -> pd.DataFrame:
  
  if not filters or not any(filters.values()):
    logger.debug("No filters to apply")
    return df
  
  for column, conditions in filters.items():
    if not conditions: # often the the list of conditions is empty, so skip
      continue
    if isinstance(conditions, list):  # if condition is a list, filter with isin
      df = df[df[column].isin(conditions)]
    else:
      logger.info(f"Filter for {column} not a list")

  return df


def get_outfilename_for_filters(filters):
  if not filters or not any(filters.values()):
    logger.debug("No filters to apply. returning empty string for outfilename.")
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