import pandas as pd
from data_config import nada_drug_days_categories, PDC_ODC_ATOMfield_names as PDC_ODC_fields
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




def convert_str_to_int_rounded(str_number:str) -> int:
    return int(round(float(str_number),0))

"""
  2nd return variable it whether a category was found or not
"""
def get_nada_drg_category(drug_name:str) -> tuple[str, int]:
  # found_category  = 0  
  for category_name, substances in nada_drug_days_categories.items():
     if drug_name in substances:
        return category_name, 1
  # print(f"no category for drug {drug_name}. ")
  # logger.error(f"no category for drug {drug_name}. ")
  return drug_name, 0

"""
    # Amphetamine Typical Qty: 
    # 0 to 999 (plus description of units) String 50
    # The average amount of amphetamine used on a typical day during the past four weeks. 
    # Agree on a meaningful unit of measure with the client. Common units of measure may include ‘grams’, 
    # number of times used per days, or the monetary value of drugs consumed. 
    # Please use the same unit of measure for subsequent survey time points.  
"""
# def get_typical_qty(item, field_names:dict[str, str], assessment):
#   field_perocc = field_names['per_occassion']
#   field_units = field_names['units']
#   empty = "0;"
#   typical_qty = item.get(field_perocc,'')
#   typical_unit = item.get(field_units,'')
#   qty_str =""
#   if not typical_qty:
#      return typical_qty, None
#   if not pd.isna(typical_qty):      
#       if typical_qty == '0':
#          return empty, 0.0
#       if typical_qty == 'Other':
#          logger.error("'Other' used for HowMuchPerOcassion. Un-reportable value", assessment['RowKey'])
#          return '', None
#       typical_qty = range_average(typical_qty)
#       qty_str = f"{typical_qty}"
#       if typical_unit:
#         qty_str = f"{qty_str}; {typical_unit} units."
#   return qty_str, typical_qty
        

def get_typical_qty(item, field_names:dict[str, str], assessment)-> tuple[float|None, str|None, str]:
  field_perocc = field_names['per_occassion']
  field_units = field_names['units']

  typical_qty = item.get(field_perocc,'')
  typical_unit = item.get(field_units,'')

  if not typical_qty:
     return typical_qty, None, ""
  if not pd.isna(typical_qty):      
      if typical_qty == '0':
         return 0.0, None, "0"
      if typical_qty == 'Other':
         logger.warn("'Other' used for HowMuchPerOcassion. Un-reportable value", assessment['RowKey'])
         return None, None, ""
      typical_qty = range_average(typical_qty)
  if not typical_unit:
     return typical_qty, "", f"{typical_qty}"

  return typical_qty, typical_unit, f"{typical_qty}; {typical_unit}"


def process_drug_list_for_assessment(pdc_odc_colname:str, assessment):
  row_data = {}
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
    nada_drug, found_category= get_nada_drg_category (substance)
    if not found_category:
       if not row_data  or not( 'Another Drug1'  in row_data) or pd.isna(row_data['Another Drug1']):
          row_data['Another Drug1'] = nada_drug
          nada_drug ='Another Drug1'
       else:
          row_data['Another Drug2'] = nada_drug
          nada_drug ='Another Drug2'
                 
    # if not field_use_ndays in item:
    #   logger.error(f"Data Quality error {field_use_ndays} not in drug({substance})dict. \
    #                SLK:{assessment['SLK']}, RowKey:{assessment['RowKey']}.")
    # if not field_perocc in item:
    #   logger.error(f"Data Quality error {field_perocc} not in drug({substance})dict. \
    #                SLK:{assessment['SLK']}, RowKey:{assessment['RowKey']}.")             
       
    row_data[ f"{nada_drug}_DaysInLast28"] = item.get(field_use_ndays,'')     
    per_occassion , typical_unit_str, typical_use_str =  get_typical_qty(item, field_names, assessment)
    row_data[   f"{nada_drug}_PerOccassionUse"] = per_occassion
    row_data [ f"{nada_drug}_Units"] = typical_unit_str
    row_data [ f"{nada_drug}_TypicalQtyStr"] = typical_use_str
    

  return row_data 


def normalize_pdc_odc(df):
  new_data = []
  for index, row in df.iterrows():
    row_data = {}
    pdc_row_data ={}
    odc_row_data ={}
    if 'PDC' in row and isinstance(row['PDC'], list):
      pdc_row_data = process_drug_list_for_assessment('PDC', row)        
    if 'ODC' in row and isinstance(row['ODC'], list):
      odc_row_data = process_drug_list_for_assessment('ODC', row)
    
    # merge the dicts
    row_data = pdc_row_data | odc_row_data
    if row_data:
        new_data.append(row_data)
    else:
        new_data.append({})
  expanded_data = pd.DataFrame(new_data, index=df.index)   
  return expanded_data
    # # Create a DataFrame from the list of dictionaries
    # expanded_data = pd.DataFrame(new_data, index=df.index)

    # # Combine the new columns with the original DataFrame
    # # result_df = pd.concat([df, expanded_data], axis=1)
    # # print(set(unique_subtances))
    # # return result_df
    # return expanded_data



def expand_drug_info(df1:pd.DataFrame) ->  pd.DataFrame:

  #  masked_rows=  df1[('ODC' in df1) and df1['ODC'].apply(lambda x: isinstance(x,list) and len(x) > 0 )]
  normd_drugs_df = normalize_pdc_odc(df1)
    
  cloned_df = df1.join(normd_drugs_df)


     
  return cloned_df
# def expand_drug_info(df1:pd.DataFrame) ->  pd.DataFrame:

#   #  masked_rows=  df1[('ODC' in df1) and df1['ODC'].apply(lambda x: isinstance(x,list) and len(x) > 0 )]
#   normd_drugs_df = normalize_pdc_odc(df1)
    
#   cloned_df = df1.copy()
#   # cloned_df.drop(['PDC', 'ODC'], inplace=True)

#   # col_index = {}
#   # my_drug_cols = list(set(normd_pdc.columns + normd_odc.columns))
#   # suffixes = ('_PerOccassionUse', '_DaysInLast28' , '_TypicalQty' )
  
#   for index, row_dict in normd_drugs_df.iterrows():
#     # Find columns with the specified suffix
#     # relevant_columns = [col for col in df.columns if any(col.endswith(suffix) for suffix in suffixes)]
#     # Check for non-empty and non-NaN values in these columns
#     # non_empty_values = {col: row_dict[col] for col in row_dict 
#     #                     if col in row_dict and pd.notna(row_dict[col])}
    
#     for drug_cat_col, val in row_dict.items():
#       cloned_df.at[index, drug_cat_col] = val

#         # new_fields_to_keep.append(col_perocc, col_daysin28)  
#   return cloned_df #, new_fields_to_keep

# def expand_drug_info(df1:pd.DataFrame) -> tuple[pd.DataFrame, list]:
#   new_fields_to_keep = []
#   #  masked_rows=  df1[('ODC' in df1) and df1['ODC'].apply(lambda x: isinstance(x,list) and len(x) > 0 )]
#   normd_pdc = normalize_multiple_elements(df1,'PDC')
#   normd_odc = normalize_multiple_elements(df1,'ODC')
  
#   cloned_df = df1.copy()
#   # cloned_df.drop(['PDC', 'ODC'], inplace=True)

#   # col_index = {}

#   for index, row in normd_pdc.iterrows():
#     for drug_cat in nada_drug_days_categories.keys():
        
#         col_perocc  = f"{drug_cat}_PerOccassionUse"
#         col_daysin28 = f"{drug_cat}_DaysInLast28"
#         # col_perocc in normd_pdc.loc[0].keys() /
#         # if either of these columns are blank in the PDC, use the ODC one
#         if pd.isnull(normd_pdc.at[index, col_perocc]) or pd.isnull(normd_pdc.at[index, col_daysin28]) :
#            cloned_df.at[index, col_perocc] = normd_odc.at[index, col_perocc]
#            cloned_df.at[index, col_daysin28] = normd_odc.at[index, col_daysin28]
#         else:
#            cloned_df.at[index, col_perocc] = normd_pdc.at[index, col_perocc]
#            cloned_df.at[index, col_daysin28] = normd_pdc.at[index, col_daysin28]
  
#   new_fields_to_keep = [col for col in cloned_df.columns
#                         if '_PerOccassionUse' in col or '_DaysInLast28' in col or '_TypicalQty'  in col] 
  
#         # new_fields_to_keep.append(col_perocc, col_daysin28)
#   return cloned_df, new_fields_to_keep

# def normalize_multiple_elements(df, column_name):
#     # unique_subtances =[]
#     # List to hold row-wise dictionaries for new data
#     new_data = []

#     for index, row in df.iterrows():
#       if column_name in row and isinstance(row[column_name], list):
#         row_data = process_drug_list_for_assessment(column_name, row)
#         new_data.append(row_data)
#       else:
#          new_data.append({})

#     # Create a DataFrame from the list of dictionaries
#     expanded_data = pd.DataFrame(new_data, index=df.index)

#     # Combine the new columns with the original DataFrame
#     # result_df = pd.concat([df, expanded_data], axis=1)
#     # print(set(unique_subtances))
#     # return result_df
#     return expanded_data

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
          [{'PDCSubstanceOrGambling': 'Cannabinoids', 'PDCDaysInLast28': '20', 'PDCHowMuchPerOccasion': 55.0,'PDCUnits': 'blunts'}],
          [{'PDCSubstanceOrGambling': 'Psychostimulants, n.f.d.', 'PDCDaysInLast28': '15'}],
          [{'PDCSubstanceOrGambling': 'Ethanol', 'PDCDaysInLast28': '15',  'PDCUnits': 'standard drinks'}],  

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
          [{ 'OtherSubstancesConcernGambling': 'Ethanol','DaysInLast28': '10','HowMuchPerOccasion': '50-59','Units': 'standard drinks'}],


          # [{'OtherSubstancesConcernGambling': 'Cocaine', 'DaysInLast28': '5'}],      
          # [],
          # [{'OtherSubstancesConcernGambling': 'Benzodiazepines, n.f.d.', 'DaysInLast28': '15', 'HowMuchPerOccasion': '2'}],      
      ]
  })

  out = expand_drug_info(df)
  # TODO : delete PDC and ODC columns
  print(out)