import os
import pandas as pd
import mylogger

logger = mylogger.get(__name__)
# List of column names in the CSV
column_names = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION', 'PMSEpisodeID', 'PMSPersonID', 'DOB', 'DOB STATUS', 'SEX', 'COUNTRY OF BIRTH', 'INDIGENOUS STATUS', 'PREFERRED LANGUAGE', 'SOURCE OF INCOME', 'LIVING ARRANGEMENT', 'USUAL ACCOMMODATION', 'CLIENT TYPE', 'PRINCIPAL DRUG OF CONCERN', 'PDCSubstanceOfConcern', 'ILLICIT USE', 'METHOD OF USE PRINCIPAL DRUG', 'INJECTING DRUG USE', 'SETTING', 'CommencementDate', 'POSTCODE', 'SOURCE OF REFERRAL', 'MAIN SERVICE', 'EndDate', 'END REASON', 'REFERRAL TO ANOTHER SERVICE', 'FAMILY NAME', 'GIVEN NAME', 'MIDDLE NAME', 'TITLE', 'SLK', 'MEDICARE NUMBER', 'PROPERTY NAME', 'UNIT FLAT NUMBER', 'STREET NUMBER', 'STREET NAME', 'SUBURB']

# List of columns we care about
columns_of_interest = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION', 'PMSEpisodeID', 'PMSPersonID', 'PDCSubstanceOfConcern', 'CommencementDate', 'EndDate', 'SLK']

def load_and_parse_csv(filepath):
    # Load the CSV
    df = pd.read_csv(filepath, header=None, names=column_names)

    # Select only the columns we care about
    df = df[columns_of_interest]

    df['CommencementDate'] = pd.to_datetime(df['CommencementDate'], format='%d%m%Y')
    df['EndDate'] = pd.to_datetime(df['EndDate'], format='%d%m%Y')    
    return df



def load_and_parse_csvs(directory):
    # List to hold dataframes
    dfs = []
    
    # Loop over all files in the directory
    for filename in os.listdir(directory):
        # Check if the file is a CSV
        if not filename.endswith('.csv'):
            continue
        filepath = os.path.join(directory, filename)
        try:
          df = load_and_parse_csv(filepath)
        except ValueError as e:
            logger.error(f"Error parsing dates in file {filepath} with error {str(e)}")
            # logger.error("The problematic row is:")

            
            continue  # Skip this file and move to the next one

        # Append the dataframe to the list
        dfs.append(df)
    
    # Concatenate all dataframes in the list
    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


# matched_df = match_assessments(episodes_df, atoms_df, pdc_substance_mapping, establishment_program_mapping)

def match_assessments(episodes_df, atoms_df, pdc_substance_mapping, establishment_program_mapping):
    # Apply the mapping to the ESTABLISHMENT IDENTIFIER and PDCSubstanceOfConcern columns in episodes_df
    episodes_df['ESTABLISHMENT IDENTIFIER'] = episodes_df['ESTABLISHMENT IDENTIFIER'].map(establishment_program_mapping)
    episodes_df['PDCSubstanceOfConcern'] = episodes_df['PDCSubstanceOfConcern'].map(pdc_substance_mapping)

    # Ensure that AssessmentDate is in datetime format
    atoms_df['AssessmentDate'] = pd.to_datetime(atoms_df['AssessmentDate'], errors='coerce')

    # Merge the dataframes on SLK and Program
    merged_df = pd.merge(episodes_df, atoms_df, how='inner', left_on=['SLK', 'ESTABLISHMENT IDENTIFIER'], right_on=['SLK', 'Program'])

    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    matched_df = merged_df.loc[((merged_df['AssessmentDate'] >= merged_df['CommencementDate']) & 
                                (merged_df['AssessmentDate'] <= merged_df['EndDate'])) |
                               ((merged_df['AssessmentDate'] >= merged_df['CommencementDate']) & 
                                (merged_df['EndDate'].isna()))]

    # Check if PDCSubstanceOfConcern matches
    mismatched_df = matched_df.loc[matched_df['PDCSubstanceOfConcern_x'] != matched_df['PDCSubstanceOfConcern_y']]

    if len(mismatched_df) > 0:
        logger.info(f"There are {len(mismatched_df)} rows where PDCSubstanceOfConcern does not match.")
        logger.info(mismatched_df)

    return matched_df
