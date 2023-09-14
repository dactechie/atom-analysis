
import pandas as pd
import mylogger
from utils.df_xtrct_prep import load_and_parse_episode_csvs, extract_prep_atom_data
from data_config import EstablishmentID_Program

logger = mylogger.get(__name__)

extract_start_date = 20200101
extract_end_date = 20240101

fname = f"{extract_start_date}_{extract_end_date}" 

active_clients_start_date ='2023-04-01' 
active_clients_end_date = '2023-06-30'

# matched_df = match_assessments(episodes_df, atoms_df, pdc_substance_mapping, establishment_program_mapping)

def match_assessments(episodes_df, atoms_df):
    # Apply the mapping to the ESTABLISHMENT IDENTIFIER and PDCSubstanceOfConcern columns in episodes_df
    # episodes_df['ESTABLISHMENT IDENTIFIER'] = episodes_df['ESTABLISHMENT IDENTIFIER'].map(establishment_program_mapping)
    # episodes_df['PDCSubstanceOfConcern'] = episodes_df['PDCSubstanceOfConcern'].map(pdc_substance_mapping)


    # Merge the dataframes on SLK and Program
    merged_df = pd.merge(episodes_df, atoms_df, how='inner', left_on=['SLK', 'Program'], right_on=['SLK', 'Program'])

    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    matched_df = merged_df.loc[((merged_df['AssessmentDate'] >= merged_df['CommencementDate']) & 
                                (merged_df['AssessmentDate'] <= merged_df['EndDate'])) |
                               ((merged_df['AssessmentDate'] >= merged_df['CommencementDate']) & 
                                (merged_df['EndDate'].isna()))]

    # Check if PDCSubstanceOfConcern matches
    # mismatched_df = matched_df.loc[matched_df['PDCSubstanceOfConcern_x'] != matched_df['PDCSubstanceOfConcern_y']]

    # if len(mismatched_df) > 0:
    #     logger.info(f"There are {len(mismatched_df)} rows where PDCSubstanceOfConcern does not match.")
    #     logger.info(mismatched_df)

    return matched_df


def get_atoms() -> pd.DataFrame:
  processed_df = extract_prep_atom_data(extract_start_date, extract_end_date
                                , active_clients_start_date
                                , active_clients_end_date
                                , fname)
  return processed_df

def process_mismatched(matched_atoms: pd.DataFrame, atoms_df:pd.DataFrame):
  # Perform an outer join
  outer_merged_df = pd.merge(matched_atoms, atoms_df, how='outer', left_on=['SLK', 'Program'], right_on=['SLK', 'Program'], indicator=True)

  # Filter rows that are only in atoms_df
  only_in_atoms_df = outer_merged_df[outer_merged_df['_merge'] == 'right_only']

  # Drop the indicator column and keep only columns from atoms_df
  only_in_atoms_df = only_in_atoms_df.drop(columns=['_merge'])
  cleaned_df =  only_in_atoms_df.dropna(axis=1, how='all')
  return cleaned_df

    

def main():
    ep_df = load_and_parse_episode_csvs("./data/in/NSW_CSV/")
    ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(EstablishmentID_Program)
    ep_df.drop('ESTABLISHMENT IDENTIFIER',  axis=1, inplace=True)

    atoms = get_atoms()
    matched = match_assessments(ep_df, atoms)

    unmatched_atoms_df = process_mismatched(matched, atoms)
    print(unmatched_atoms_df)
    # atoms[atoms.Staff.notna() ] # only 131



if __name__ == "__main__":
    main()