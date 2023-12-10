from utils.io import read_csv_to_dataframe
from utils.ccare_to_aztable import adjust_ccare_csv_for_aztable
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
  new_data = adjust_ccare_csv_for_aztable(data)
  tableObject = SampleTablesQuery(table_name)
  tableObject.batch_insert_data(new_data)
  # ERROR - An unexpected error occurred: unsupported operand type(s) for +=: 'NoneType' and 'str'
   

def main(env='local'):
  MyEnvironmentConfig().setup(env)
  source_file = "NSW_CSV/DATS_AllPrograms_01072023-30122023.csv" #"ForAzTabl_CcareAutomationReport_MDS_1Jul2018-30Sep2023-AllPrograms.csv"
  
  fetch_insert_data('MDSMatchingAudit', source_file)


if __name__ == "__main__":
  #  ep_df = main2() #'prod')
   main()#'prod')
