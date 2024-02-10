from datetime import datetime
import numpy as np

def convert_format_datestr(date_string:str, from_format:str, to_format:str):

  date1 = datetime.strptime(date_string, from_format).strftime(to_format)
  return date1

def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
    
    
# def range_average(range_str:str, separator:str='-'):
  
#   if is_numeric(range_str):
#     return float(range_str)  
  
#   elif separator in range_str:
#     two_ints = range_str.split(separator)
#     return (int(two_ints[0])+int(two_ints[1]))/2
   
#   else:
#     return np.nan
  
def range_average(range_str:str, separator:str='-'):
  
  if is_numeric(range_str):    
    return int(np.ceil(float(range_str)))
  
  elif separator in range_str:
    two_ints = range_str.split(separator)
    return int(round((int(two_ints[0])+int(two_ints[1]))/2,0))
   
  else:
    return np.nan    
  

# Function to safely parse JSON and handle errors
def clean_and_parse_json(s:str):
    import json
    import mylogger
    logger = mylogger.get(__name__)
    try:
        cleaned_string = s.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        return json.loads(cleaned_string)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        logger.error(f"Problematic data: {s}")
        # Return None or some default value if JSON is invalid
        return None