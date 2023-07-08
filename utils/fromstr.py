from datetime import datetime
import numpy as np

def get_date_from_yyyymmdd(yyyymmdd_str:str):

  date1 = datetime.strptime(yyyymmdd_str, '%Y%m%d').strftime('%d/%m/%Y')
  return date1


def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
    
    
def range_average(range_str:str, separator:str='-'):
  
  if is_numeric(range_str):
    return float(range_str)  
  
  elif separator in range_str:
    two_ints = range_str.split(separator)
    return (int(two_ints[0])+int(two_ints[1]))/2
   
  else:
    return np.nan    