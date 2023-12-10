
import pandas as pd
import mylogger
from utils.df_xtrct_prep import extract_prep_atom_data, extract_prep_episode_data

from utils.environment import MyEnvironmentConfig

logger = mylogger.get(__name__)

# def min_gaps_from_boundaries(atoms_df, eps_df ):#assessment_dates:pd.Series, eps_start:pd.Series, eps_end:pd.Series):
   

# def match_exclude(atoms, eps):


# def first_last_atoms_inepisode():
   
#     # Merge using inner join, preserving indices
#     merged_df = pd.merge(df1, df2, how='inner', left_index=True, right_index=True)

#     first_assessment_idx = matched_df.groupby(['SLK', 'Program', 'CommencementDate', 'EndDate'])['AssessmentDate'].idxmin()
#     last_assessment_idx = matched_df.groupby(['SLK', 'Program', 'CommencementDate', 'EndDate'])['AssessmentDate'].idxmax()
#     # Create new columns to flag the first and last AssessmentDate within each episode
#     matched_df['is_first_assessment'] = 0  # Initialize with zeros
#     matched_df['is_last_assessment'] = 0  # Initialize with zeros

#     # Set flags for first and last assessments
#     matched_df.loc[first_assessment_idx, 'is_first_assessment'] = 1
#     matched_df.loc[last_assessment_idx, 'is_last_assessment'] = 1



def get_mask_datefit(assessment_date: pd.Series, episode_commencement_date: pd.Series, episode_end_date: pd.Series, slack_days=30):
    slack_td = pd.Timedelta(days=slack_days)

    after_commencement = assessment_date >= (episode_commencement_date - slack_td) 
                           
    after_start_ongoing_ep = pd.isna(episode_end_date) & after_commencement

    within_ep_dates = pd.notna(episode_end_date) & after_commencement & \
                      (assessment_date <= (episode_end_date +slack_td))

    return within_ep_dates | after_start_ongoing_ep

# def assessment_fit_episode(assessment_date: pd.Series, episode_commencement_date: pd.Series, episode_end_date: pd.Series, slack_days=30):
#     if
#     return (assessment_date >= (episode_commencement_date - pd.Timedelta(days=slack_days))) & \
#            (assessment_date <= (episode_end_date + pd.Timedelta(days=slack_days)))

def match_assessments(episodes_df, atoms_df, matching_ndays_slack:int):

    # Merge the dataframes on SLK and Program
    df = pd.merge(episodes_df, atoms_df, how='inner', left_on=['SLK', 'Program'], right_on=['SLK', 'Program'])

    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    
    mask = get_mask_datefit(df['AssessmentDate'], df['CommencementDate'], df['EndDate'], slack_days=matching_ndays_slack)
    filtered_df = df[mask]
    # matched_df = merged_df.loc[((merged_df['AssessmentDate'] >= merged_df['CommencementDate']) & 
    #                             (merged_df['AssessmentDate'] <= merged_df['EndDate'])) |
    #                            ((merged_df['AssessmentDate'] >= merged_df['CommencementDate']) & 
    #                             (merged_df['EndDate'].isna()))]

    # Check if PDCSubstanceOfConcern matches
    # mismatched_df = matched_df.loc[matched_df['PDCSubstanceOfConcern_x'] != matched_df['PDCSubstanceOfConcern_y']]

    # if len(mismatched_df) > 0:
    #     logger.info(f"There are {len(mismatched_df)} rows where PDCSubstanceOfConcern does not match.")
    #     logger.info(mismatched_df)

    return filtered_df

"""
2nd param : all_atoms_df or all_eps_df
"""
def process_mismatched(matched_atoms: pd.DataFrame, atoms_df:pd.DataFrame):
  # Perform an outer join
  outer_merged_df = pd.merge(matched_atoms, atoms_df, how='outer', left_on=['SLK', 'Program'], right_on=['SLK', 'Program'], indicator=True)

  # Filter rows that are only in atoms_df
  only_in_atoms_df = outer_merged_df[outer_merged_df['_merge'] == 'right_only']
  

  # Drop the indicator column and keep only columns from atoms_df
  only_in_atoms_df = only_in_atoms_df.drop(columns=['_merge'])
  cleaned_df =  only_in_atoms_df.dropna(axis=1, how='all')
  return cleaned_df



def get_eps_unmatched_atoms(atoms:pd.DataFrame,
                      matched:pd.DataFrame, ep_df:pd.DataFrame
                            ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    unmatched_atoms_df = process_mismatched(matched, atoms)    
    column_mapping = { 'Staff_y': 'Staff', 'AssessmentDate_y': 'AssessmentDate', 'PDC_y' : 'PDC'}

    new_unmatched_atomdf = unmatched_atoms_df.rename(columns=column_mapping)
    new_unmatched_atomdf = new_unmatched_atomdf[['SLK', 'Program', 'Staff', 'AssessmentDate', 'PDC']]
    

    return ep_df, new_unmatched_atomdf
    # atoms[atoms.Staff.notna() ] # only 131



def extract_atom_n_episodes (extract_start_date, extract_end_date) -> tuple[pd.DataFrame, pd.DataFrame]:
  fname = f"{extract_start_date}-{extract_end_date}" 

  active_clients_start_date = None #'2020-01-01' 
  active_clients_end_date = None #'2023-09-15'  
  processed_atom_df = extract_prep_atom_data(extract_start_date, extract_end_date
                              , active_clients_start_date
                              , active_clients_end_date
                              , fname, purpose='Matching')
  processed_ep_df = extract_prep_episode_data(extract_start_date, extract_end_date
                              , f"Matching_eps_{fname}")  
  return processed_atom_df, processed_ep_df



def attempt_match(atoms:pd.DataFrame, processed_ep_df:pd.DataFrame):
  
  matched = match_assessments(processed_ep_df, atoms, MyEnvironmentConfig().matching_ndays_slack)
  # processed_ep_df = load_and_parse_episode_csvs("./data/in/NSW_CSV/")  
  ep_df, unmatched_atoms_df = get_eps_unmatched_atoms(atoms, matched, processed_ep_df)

  return ep_df, unmatched_atoms_df


def program_mismatch(ep_df:pd.DataFrame, unmatched_atoms_df:pd.DataFrame) -> pd.DataFrame:
 unmatched_w_slk_match = unmatched_atoms_df.loc[unmatched_atoms_df['SLK'].isin(ep_df['SLK'])]
 # Merge on 'SLK'
 merged_df = pd.merge(unmatched_w_slk_match, ep_df, on='SLK', suffixes=('_atom', '_ep'))
 # Find rows where 'Program' values don't match
 mismatch_df = merged_df[merged_df['Program_atom'] != merged_df['Program_ep']]
#  program_mismatch_for_matched_slk = mismatch_df[['SLK','Program_atom', 'Program_ep','Staff',	'AssessmentDate','CommencementDate', 'EndDate']].drop_duplicates()
 program_mismatch_for_matched_slk = mismatch_df[['SLK','Program_atom', 'Program_ep','Staff',	'AssessmentDate']].drop_duplicates()
 return program_mismatch_for_matched_slk


# class MatchingAudit:
#   NoEpisodeForSLK: pd.DataFrame
#   ProgramMismatch: pd.DataFrame

#   # result of  atoms, processed_ep_df = extract_atom_n_episodes(extract_start_date, extract_end_date)
#   def __init__(self, atoms, processed_ep_df):
#      atoms, processed_ep_df

#   def get_atoms_without_ep_slk(self):
    

"""
Writes the output to csv files
"""
def write_audit_results_csv(results, period_str:str):
  #write results to csv
  parent_folder = "data/out/audits"
  results['NoEpisodeForSLK'].to_csv(f'{parent_folder}/atoms__noep_for_slk_{period_str}.csv', index=False)
  results['ProgramMismatch'].to_csv(f'{parent_folder}/program_mismatch_for_matched_slk_{period_str}.csv', index=False)   

"""
do all audits  and return results
"""
def do_audits(ep_df, unmatched_atoms_df):
  #SLK in atom but not in Episodes
  atoms__noep_for_slk = unmatched_atoms_df.loc[~unmatched_atoms_df['SLK'].isin(ep_df['SLK'])]
  
  program_mismatch_for_matched_slk = program_mismatch(ep_df, unmatched_atoms_df)

  return { "NoEpisodeForSLK": atoms__noep_for_slk, "ProgramMismatch": program_mismatch_for_matched_slk}
  # return unmatched_atoms_df


def main(env='local'):
  MyEnvironmentConfig().setup(env)
  extract_start_date =  20230701 # 20230501 #20220701
  extract_end_date = 20231230
  
  atoms, processed_ep_df = extract_atom_n_episodes(extract_start_date, extract_end_date)
  ep_df, unmatched_atoms_df = attempt_match(atoms, processed_ep_df)
  audit_results = do_audits(ep_df, unmatched_atoms_df)

  write_audit_results_csv(audit_results, f"{extract_start_date}-{extract_end_date}")
  return ep_df, unmatched_atoms_df,  audit_results

  # MatchTmp_MatchedSLKEpisodeProgram




# def main2(env='local'):
#   MyEnvironmentConfig().setup(env)
#   extract_start_date = 20230501 #20220701
#   extract_end_date = 20230930

#   fname = f"{extract_start_date}_{extract_end_date}" 

#   active_clients_start_date ='2020-01-01' 
#   active_clients_end_date = '2023-09-15'   
#   processed_ep_df = extract_prep_episode_data(extract_start_date, extract_end_date
#                               , active_clients_start_date
#                               , active_clients_end_date
#                               , fname)  


#   return processed_ep_df

if __name__ == "__main__":
  #  ep_df = main2() #'prod')
  ep_df, unmatched_atoms_df, results = main('prod')
  print(ep_df)
  print(unmatched_atoms_df)