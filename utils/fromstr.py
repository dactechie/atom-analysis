from datetime import datetime


def get_date_from_yyyymmdd(yyyymmdd_str:str):

  date1 = datetime.strptime(yyyymmdd_str, '%Y%m%d').strftime('%d/%m/%Y')
  return date1