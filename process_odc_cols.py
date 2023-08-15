import pandas as pd
from data_config import drug_categories, PDC_ODC_fields




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
    'Alcohol': ['Ethanol'],
    'Heroin': ['Heroin'],
    'Other Opioids': ['Oxycodone','Pharmaceutical Opioids','Pharmaceutical Opioids, n.f.d.', 'Methadone', 'Opioid Antagonists, nec'],
    # Fentanyl, Tramadol, COdeine, Morphine

    'Cocaine': ['Cocaine'],
    'Cannabis': ['Cannabinoids and Related Drugs, n.f.d.', 'Cannabinoids'],
    'Amphetamines': ['Amphetamines, n.f.d.', 'Amphetamines, n.f.d', 'Methamphetamine'],
    'Tranquilliser': ['Benzodiazepines, nec', 'Benzodiazepines, n.f.d', 'Benzodiazepines, n.f.d.',  'Diazepam'],

    'Another Drug':  ['Other', 'Psychostimulants, n.f.d.','Zolpidem', 'Caffeine',  'MDMA/Ecstasy',  ],
}

def convert_str_to_int_rounded(str_number:str) -> int:
    return int(round(float(str_number),0))

# Function to extract the PDCDaysInLast28 value for a given category
def extract_days(survey_druguse_list, substances, field_type:dict) -> int | None:
    if not survey_druguse_list:
       return None
    
    field_name = field_type['FIELD']
    field_ndays = field_type['NDAYS_FIELD']
    
    # matched_category_ndays = [item[field_ndays] for item in survey_data  if item[field_name] in substances]

    for item in survey_druguse_list:
      if item[field_name] in substances:
        return convert_str_to_int_rounded(item[field_ndays])
        # else:
        #     print(f"Item not found: {item[field_name]} in substances: {substances}")
    return None

"""
  Function to expand the drug information in the PDC and ODC columns
  into  Drug categories defined by NADA's survey.txt output
  and the value for :'Number of days used in past 28'
"""

def expand_drug_info(df1:pd.DataFrame) -> pd.DataFrame:
  df = df1.copy()
  # Iterate through categories and create new columns
  field_type = PDC_ODC_fields['ODC']
  for category_name, substances in drug_categories.items():
    cat_series = df['ODC'].apply(lambda row: extract_days(row, substances, field_type))
    if cat_series.any():
      df[category_name] = cat_series.dropna() #.squeeze()

  # NOTE : I thought the PDC was made into just the first row, so not a list
  field_type = PDC_ODC_fields['PDC']
  for category_name, substances in drug_categories.items():
      cat_series = df['PDC'].apply(lambda row: extract_days(row, substances, field_type))
      if cat_series.any():
        if category_name in df.columns:
          df[category_name].update(cat_series.dropna())
        else:
          df[category_name] = cat_series.dropna()

  return df

if __name__ == "__main__":
  # Sample DataFrame
  df = pd.DataFrame({
      'PDC': [
          [{'PDCSubstanceOrGambling': 'Cannabinoids', 'PDCDaysInLast28': '20'}],
          [{'PDCSubstanceOrGambling': 'Psychostimulants, n.f.d.', 'PDCDaysInLast28': '15'}],
          [{'PDCSubstanceOrGambling': 'Ethanol', 'PDCDaysInLast28': '15'}],  
          [{'PDCSubstanceOrGambling': 'Caffeine', 'PDCDaysInLast28': '15'}],  
          [{'PDCSubstanceOrGambling': 'Ethanol', 'PDCDaysInLast28': '25'}],  
          
      ],
      'ODC': [
          [{'OtherSubstancesConcernGambling': 'Ethanol', 'DaysInLast28': '10'}],
          None,
          [{'OtherSubstancesConcernGambling': 'Cocaine', 'DaysInLast28': '5'}],      
          [],
          [{'OtherSubstancesConcernGambling': 'Benzodiazepines, n.f.d.', 'DaysInLast28': '15'}],      
      ]
  })

  out = expand_drug_info(df)
  # TODO : delete PDC and ODC columns
  print(out)