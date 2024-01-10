import pandas as pd
from data_config import drug_categories, PDC_ODC_ATOMfield_names as PDC_ODC_fields
from utils.fromstr import range_average
import mylogger
logger = mylogger.get(__name__)


# TODO: use NADA fields
# DU Heroin use number of days
# DU Other opioid use number of days
# DU Cannabis use number of days
# DU Cocaine use number of days
# DU Amphetamine use number of days
# DU Tranquilliser use number of days
# DU Another drug use number of days
# DU Alcohol use number of days


# Define the categories and their corresponding names in the data
nada_drug_days_categories = {
    'Alcohol': ['Ethanol','Alcohols, n.e.c.'],
    'Heroin': ['Heroin'],
    'Other Opioids': ['Oxycodone','Pharmaceutical Opioids','Pharmaceutical Opioids, n.f.d.', 'Methadone', 'Opioid Antagonists, nec'],
    # Fentanyl, Tramadol, COdeine, Morphine

    'Cocaine': ['Cocaine'],
    'Cannabis': ['Cannabinoids and Related Drugs, n.f.d.', 'Cannabinoids and related drugs, n.f.d.', 'Cannabinoids'],
    'Amphetamines': ['Amphetamines, n.f.d.', 'Amphetamines, n.f.d', 'Methamphetamine'],
    'Tranquilliser': ['Benzodiazepines', 'Benzodiazepines, nec', 'Benzodiazepines, n.e.c.', 'Benzodiazepines, n.f.d', 'Benzodiazepines, n.f.d.',  'Diazepam'],

    'Another Drug':  [
       'Opioid Antagonists, n.e.c.','Volatile Nitrates, n.e.c.', 'Lithium',
       'Other','Other Drugs of Concern', 'Psychostimulants, n.f.d.','Zolpidem', 'Caffeine',  'MDMA/Ecstasy',  
                        'Gamma-hydroxybutyrate','Dexamphetamine','GHB type Drugs and Analogues, n.e.c.',
                        'Psilocybin or Psilocin', 'Amyl nitrate', 'Other Volatile Solvents, n.e.c.' ],
    'Nicotine': ['Nicotine'],
    'Gambling':['Gambling']
}

def convert_str_to_int_rounded(str_number:str) -> int:
    return int(round(float(str_number),0))

def get_nada_drg_category(drug_name:str):
  for category_name, substances in nada_drug_days_categories.items():
     if drug_name in substances:
        return category_name
  print(f"no category for drug {drug_name}. ")
  logger.error(f"no category for drug {drug_name}. ")
  return drug_name

"""
    # Amphetamine Typical Qty: 
    # 0 to 999 (plus description of units) String 50
    # The average amount of amphetamine used on a typical day during the past four weeks. 
    # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, 
    # number of times used per days, or the monetary value of drugs consumed. 
    # Please use the same unit of measure for subsequent survey time points.  
"""
def get_typical_qty(item, field_names:dict[str, str]) -> str:
  field_perocc = field_names['per_occassion']
  field_units = field_names['units']
  typical_qty = item.get(field_perocc,'')
  typical_unit = item.get(field_units,'')
  qty_str =""
  # if typical_qty:
  if not pd.isna(typical_qty):
      if typical_qty == '0':
         return "0;"
      typical_qty = range_average(typical_qty)
      qty_str = f"{typical_qty}"
      if typical_unit:
        qty_str = f"{qty_str}; {typical_unit} units."
  return qty_str
        
def process_drug_list_for_assessment(pdc_odc_colname:str, assessment):
  row_data = {}
  # unique_subtances = []
  for item in assessment[pdc_odc_colname]: #'ODC: []' in row
    # Extract data for each substance
    field_names  = PDC_ODC_fields[pdc_odc_colname]
    field_drug_name = field_names['drug_name']
    field_use_ndays = field_names['used_in_last_4wks']

    if not item: # {}
      continue

    substance = item.get(field_drug_name, '')    
    if not substance:
      # logger.error(f"Data Quality error {field_drug_name} not in drug dict. SLK:{assessment['PartitionKey']}, RowKey:{assessment['RowKey']}.")
      continue
    # unique_subtances.append(substance) 
    nada_drug = get_nada_drg_category (substance)
    # if not field_use_ndays in item:
    #   logger.error(f"Data Quality error {field_use_ndays} not in drug({substance})dict. \
    #                SLK:{assessment['SLK']}, RowKey:{assessment['RowKey']}.")
    # if not field_perocc in item:
    #   logger.error(f"Data Quality error {field_perocc} not in drug({substance})dict. \
    #                SLK:{assessment['SLK']}, RowKey:{assessment['RowKey']}.")             
       
    row_data[ f"{nada_drug}_DaysInLast28"] = item.get(field_use_ndays,'')     
    row_data[ f"{nada_drug}_PerOccassionUse"] = get_typical_qty(item, field_names)

    # for key, value in item.items():
    #     if key != field_drug_name:
    #         # Create new column names and values                    
    #         row_data[ f"{nada_drug}_{key}"] = value
  return row_data #, unique_subtances

def normalize_multiple_elements(df, column_name):
    # unique_subtances =[]
    # List to hold row-wise dictionaries for new data
    new_data = []

    for index, row in df.iterrows():
      if column_name in row and isinstance(row[column_name], list):
        row_data = process_drug_list_for_assessment(column_name, row)
        new_data.append(row_data)
      else:
         new_data.append({})

    # Create a DataFrame from the list of dictionaries
    expanded_data = pd.DataFrame(new_data, index=df.index)

    # Combine the new columns with the original DataFrame
    # result_df = pd.concat([df, expanded_data], axis=1)
    # print(set(unique_subtances))
    # return result_df
    return expanded_data

def expand_drug_info(df1:pd.DataFrame) -> tuple[pd.DataFrame, list]:
  new_fields_to_keep = []
  #  masked_rows=  df1[('ODC' in df1) and df1['ODC'].apply(lambda x: isinstance(x,list) and len(x) > 0 )]
  normd_pdc = normalize_multiple_elements(df1,'PDC')
  normd_odc = normalize_multiple_elements(df1,'ODC')
  
  cloned_df = df1.copy()
  # cloned_df.drop(['PDC', 'ODC'], inplace=True)

  # col_index = {}

  for index, row in normd_pdc.iterrows():
    for drug_cat in nada_drug_days_categories.keys():
        
        col_perocc  = f"{drug_cat}_PerOccassionUse"
        col_daysin28 = f"{drug_cat}_DaysInLast28"
        # col_perocc in normd_pdc.loc[0].keys() /
        # if either of these columns are blank in the PDC, use the ODC one
        if pd.isnull(normd_pdc.at[index, col_perocc]) or pd.isnull(normd_pdc.at[index, col_daysin28]) :
           cloned_df.at[index, col_perocc] = normd_odc.at[index, col_perocc]
           cloned_df.at[index, col_daysin28] = normd_odc.at[index, col_daysin28]
        else:
           cloned_df.at[index, col_perocc] = normd_pdc.at[index, col_perocc]
           cloned_df.at[index, col_daysin28] = normd_pdc.at[index, col_daysin28]
  
  new_fields_to_keep = [col for col in cloned_df.columns
                        if '_PerOccassionUse' in col or '_DaysInLast28' in col] 
  
        # new_fields_to_keep.append(col_perocc, col_daysin28)
  return cloned_df, new_fields_to_keep

if __name__ == "__main__":
  # Sample DataFrame
  df = pd.DataFrame({
      'PartitionKey':[
         'ABC',
         'DEF',
         'GHI',
         'JKL',
         'MNO',
         'PQR'
      ],
      'RowKey':[
         'rkABC',
         'rkDEF',
         'rkGHI',
         'rkJKL',
         'rkMNO',
         'rkPQR'
      ],
      'PDC': [
          [{'PDCSubstanceOrGambling': 'Cannabinoids', 'PDCDaysInLast28': '20', 'PDCHowMuchPerOccasion': 55.0}],
          [{'PDCSubstanceOrGambling': 'Psychostimulants, n.f.d.', 'PDCDaysInLast28': '15'}],
          [{'PDCSubstanceOrGambling': 'Ethanol', 'PDCDaysInLast28': '15'}],  

          [{'PDCSubstanceOrGambling': 'Ethanol', 'PDCDaysInLast28': '115'}],  
          [{'PDCSubstanceOrGambling': 'Caffeine', 'PDCDaysInLast28': '15', 'PDCHowMuchPerOccasion': 25.0}],  
          [{'PDCSubstanceOrGambling': 'Ethanol', 'PDCDaysInLast28': '25'}],  
          
      ],
      'ODC': [
          [{'OtherSubstancesConcernGambling': 'Caffeine', 'DaysInLast28': '10', 'HowMuchPerOccasion': '50-59' }],          
           None,
           [{ 'DaysInLast28': '10', 'HowMuchPerOccasion': '50-59' }],
          
          [],
          [{ 'OtherSubstancesConcernGambling': 'Ethanol', 'HowMuchPerOccasion': '50-59' }],
          [{ 'OtherSubstancesConcernGambling': 'Ethanol','DaysInLast28': '10',}],


          # [{'OtherSubstancesConcernGambling': 'Cocaine', 'DaysInLast28': '5'}],      
          # [],
          # [{'OtherSubstancesConcernGambling': 'Benzodiazepines, n.f.d.', 'DaysInLast28': '15', 'HowMuchPerOccasion': '2'}],      
      ]
  })

  out = expand_drug_info(df)
  # TODO : delete PDC and ODC columns
  print(out)