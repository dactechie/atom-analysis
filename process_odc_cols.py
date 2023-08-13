import pandas as pd

field_group = {
    'ODC': {
      'FIELD' : 'OtherSubstancesConcernGambling',
      'NDAYS_FIELD' : 'DaysInLast28'  
    },
    'PDC':{
      'FIELD' : 'PDCSubstanceOrGambling',
      'NDAYS_FIELD' : 'PDCDaysInLast28'
    }
}




# Sample DataFrame
df = pd.DataFrame({
    'PDC': [
        [{'PDCSubstanceOrGambling': 'Cannabinoids', 'PDCDaysInLast28': '20'}],
        [{'PDCSubstanceOrGambling': 'Psychostimulants, n.f.d.', 'PDCDaysInLast28': '15'}],      
    ],
    'ODC': [
        [{'OtherSubstancesConcernGambling': 'Ethanol', 'DaysInLast28': '10'}],
        [{'OtherSubstancesConcernGambling': 'Cocaine', 'DaysInLast28': '5'}],      
    ]
})

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


# Function to extract the PDCDaysInLast28 value for a given category
def extract_days(row, substances, field_type:dict):
    field_name = field_type['FIELD']
    field_ndays = field_type['NDAYS_FIELD']
    for item in row:
        if item[field_name] in substances:            
            return item[field_ndays]
        else:
            print(f"Item not found: {item[field_ndays]}")
    return None

# Iterate through categories and create new columns
field_type = field_group['ODC']
for category_name, substances in nada_drug_days_categories.items():
    df[category_name] = df['ODC'].apply(lambda row: extract_days(row, substances, field_type))

field_type = field_group['PDC']
# for category_name, substances in nada_drug_days_categories.items():
#     if
# df[category_name] = df['PDC'].apply(lambda row: extract_days(row, substances))
# Resulting DataFrame
print(df)