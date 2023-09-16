
import pandas as pd
import mylogger
from utils.df_xtrct_prep import load_and_parse_episode_csvs, extract_prep_atom_data
from data_config import EstablishmentID_Program
from utils.environment import MyEnvironmentConfig

logger = mylogger.get(__name__)

# EP CSV should be taken from here :
# C:\Users\aftab.jalal\Directions Health\Directions Health Intranet - Reporting\NADASurveyGenerator\2023_csv\2023_csv
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


def process_mismatched(matched_atoms: pd.DataFrame, atoms_df:pd.DataFrame):
  # Perform an outer join
  outer_merged_df = pd.merge(matched_atoms, atoms_df, how='outer', left_on=['SLK', 'Program'], right_on=['SLK', 'Program'], indicator=True)

  # Filter rows that are only in atoms_df
  only_in_atoms_df = outer_merged_df[outer_merged_df['_merge'] == 'right_only']

  # Drop the indicator column and keep only columns from atoms_df
  only_in_atoms_df = only_in_atoms_df.drop(columns=['_merge'])
  cleaned_df =  only_in_atoms_df.dropna(axis=1, how='all')
  return cleaned_df


def get_eps_unmatched_atoms(atoms:pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    ep_df = load_and_parse_episode_csvs("./data/in/NSW_CSV/")
    ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(EstablishmentID_Program)  
    # ep_df.drop('ESTABLISHMENT IDENTIFIER',  axis=1, inplace=True)

    ep_df['PDC'] = ep_df['PDCSubstanceOfConcern']
    ep_df= ep_df[['SLK', 'Program',  'CommencementDate', 'EndDate', 'PDC']]
    atoms['PDC'] = atoms['PDCSubstanceOrGambling']
    atoms= atoms[['SLK', 'Program', 'Staff', 'AssessmentDate', 'PDC']]

    matched = match_assessments(ep_df, atoms)

    unmatched_atoms_df = process_mismatched(matched, atoms)    
    column_mapping = { 'Staff_y': 'Staff', 'AssessmentDate_y': 'AssessmentDate', 'PDC_y' : 'PDC'}

    new_unmatched_atomdf = unmatched_atoms_df.rename(columns=column_mapping)
    new_unmatched_atomdf = new_unmatched_atomdf[['SLK', 'Program', 'Staff', 'AssessmentDate', 'PDC']]
    

    return ep_df, new_unmatched_atomdf
    # atoms[atoms.Staff.notna() ] # only 131


def download_attempt_match():  
  extract_start_date = 20220701
  extract_end_date = 20230630

  fname = f"{extract_start_date}_{extract_end_date}" 

  active_clients_start_date ='2020-01-01' 
  active_clients_end_date = '2023-09-15'  
  processed_df = extract_prep_atom_data(extract_start_date, extract_end_date
                              , active_clients_start_date
                              , active_clients_end_date
                              , fname)
  ep_df, unmatched_atoms_df = get_eps_unmatched_atoms(processed_df)

  return ep_df, unmatched_atoms_df


def program_mismatch(ep_df:pd.DataFrame, unmatched_atoms_df:pd.DataFrame) -> pd.DataFrame:
 unmatched_w_slk_match = unmatched_atoms_df.loc[unmatched_atoms_df['SLK'].isin(ep_df['SLK'])]
 # Merge on 'SLK'
 merged_df = pd.merge(unmatched_w_slk_match, ep_df, on='SLK', suffixes=('_atom', '_ep'))
 # Find rows where 'Program' values don't match
 mismatch_df = merged_df[merged_df['Program_atom'] != merged_df['Program_ep']]
 program_mismatch_for_matched_slk = mismatch_df[['SLK','Program_atom',	 'Program_ep','Staff',	'AssessmentDate','CommencementDate', 'EndDate']].drop_duplicates()
 return program_mismatch_for_matched_slk
 
 
def mismatch_analysis(ep_df, unmatched_atoms_df):
  # ep_df, unmatched_atoms_df = download_attempt_match()
  #SLK in atom but not in Episodes
  atoms__noep_for_slk = unmatched_atoms_df.loc[~unmatched_atoms_df['SLK'].isin(ep_df['SLK'])]
  
  program_mismatch_for_matched_slk = program_mismatch(ep_df, unmatched_atoms_df)

  #write results to csv
  atoms__noep_for_slk.to_csv('atoms__noep_for_slk.csv', index=False)
  program_mismatch_for_matched_slk.to_csv('program_mismatch_for_matched_slk.csv', index=False)

  return atoms__noep_for_slk, program_mismatch_for_matched_slk
  # return unmatched_atoms_df


def main(env='local'):
   MyEnvironmentConfig().setup(env)
   ep_df, unmatched_atoms_df = download_attempt_match()
   noep_for_slk, prog_mismatch_for_slk = mismatch_analysis(ep_df, unmatched_atoms_df)
   return ep_df, unmatched_atoms_df,  noep_for_slk, prog_mismatch_for_slk 

if __name__ == "__main__":
  ep_df, unmatched_atoms_df, noep_for_slk, prog_mismatch_for_slk = main('prod')
  print(ep_df)
  print(unmatched_atoms_df)  