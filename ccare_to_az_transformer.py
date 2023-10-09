from utils.io import read_csv_to_dataframe
from azutil.az_tables_query import SampleTablesQuery
import mylogger
from utils.environment import MyEnvironmentConfig

logger = mylogger.get(__name__)

"""
  getting data into MDS table in Azure Table storage
  1. Run report in CCare 
      NSW CCare:  
        Ccare Report: All MonthlyForAutomation
        Right click report and Edit
        Modify dates to extract data : 'Enter_First_Date_of_Report' 
        Double-Click , Choose Export, Save CSV file to data/in folder
        E.g. ForAzTabl_CcareAutomationReport_MDS_1Jul2018-30Sep2023-AllPrograms.csv


"""


def fetch_insert_data(table_name:str, source_file: str):
  data = read_csv_to_dataframe(f'./data/in/{source_file}')
  tableObject = SampleTablesQuery(table_name)
  tableObject.batch_insert_data(data)
   

def main(env='local'):
  MyEnvironmentConfig().setup(env)
  source_file = "ForAzTabl_short.csv" #"ForAzTabl_CcareAutomationReport_MDS_1Jul2018-30Sep2023-AllPrograms.csv"
  
  fetch_insert_data('MDSMatchingAudit', source_file)


if __name__ == "__main__":
  #  ep_df = main2() #'prod')
   main()#'prod')
