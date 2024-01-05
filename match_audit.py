
import pandas as pd
import mylogger
from utils.df_xtrct_prep import extract_prep_atom_data, extract_prep_episode_data
from utils.df_ops_base import get_right_only, get_lr_mux_unmatched
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


# def get_mask_datefit(assessment_date: pd.Series, episode_commencement_date: pd.Series, episode_end_date: pd.Series, slack_days=30):
#     slack_td = pd.Timedelta(days=slack_days)

#     after_commencement = assessment_date >= (
#         episode_commencement_date - slack_td)

#     after_start_ongoing_ep = pd.isna(episode_end_date) & after_commencement

#     within_ep_dates = pd.notna(episode_end_date) & after_commencement & \
#         (assessment_date <= (episode_end_date + slack_td))

#     return within_ep_dates | after_start_ongoing_ep



def get_mask_datefit(row, slack_days=30):
    # Convert to datetime if not already in that format
    # assessment_date = pd.to_datetime(row['AssessmentDate'], errors='coerce')
    # commencement_date = pd.to_datetime(row['CommencementDate'], errors='coerce')
    # end_date = pd.to_datetime(row['EndDate'], errors='coerce')

    # Create a Timedelta for slack days
    slack_td = pd.Timedelta(days=slack_days)

    # Check conditions
    after_commencement = row['AssessmentDate'].date() >= (row['CommencementDate'] - slack_td)
    before_end_date = row['AssessmentDate'].date() <= (row['EndDate'] + slack_td)

    return after_commencement and before_end_date

# def assessment_fit_episode(assessment_date: pd.Series, episode_commencement_date: pd.Series, episode_end_date: pd.Series, slack_days=30):
#     if
#     return (assessment_date >= (episode_commencement_date - pd.Timedelta(days=slack_days))) & \
#            (assessment_date <= (episode_end_date + pd.Timedelta(days=slack_days)))


def match_assessments(episodes_df, atoms_df, matching_ndays_slack: int):

    # Merge the dataframes on SLK and Program
    df = pd.merge(episodes_df, atoms_df, how='inner', left_on=[
                  'SLK', 'Program'], right_on=['SLK', 'Program'])

    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    mask = df.apply(get_mask_datefit, slack_days=matching_ndays_slack, axis=1)
    # mask = get_mask_datefit(df['AssessmentDate'], df['CommencementDate'],
    #                         df['EndDate'], slack_days=matching_ndays_slack)
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


def get_unmatched_atoms(atoms: pd.DataFrame,
                        matched: pd.DataFrame
                        ) -> pd.DataFrame:
    unmatched_atoms_df = get_right_only(
        matched, atoms, join_cols=['SLK', 'Program'])
    column_mapping = {'Staff_y': 'Staff',
                      'AssessmentDate_y': 'AssessmentDate', 'PDC_y': 'PDC'}

    new_unmatched_atomdf = unmatched_atoms_df.rename(columns=column_mapping)
    new_unmatched_atomdf = new_unmatched_atomdf[[
        'SLK', 'Program', 'Staff', 'AssessmentDate', 'PDC']]

    return new_unmatched_atomdf
    # atoms[atoms.Staff.notna() ] # only 131


def get_matched_unmatched_atoms(atoms: pd.DataFrame, processed_ep_df: pd.DataFrame):

    matched = match_assessments(
        processed_ep_df, atoms, MyEnvironmentConfig().matching_ndays_slack)
    # processed_ep_df = load_and_parse_episode_csvs("./data/in/NSW_CSV/")
    unmatched_atoms_df = get_unmatched_atoms(atoms, matched)

    return unmatched_atoms_df, matched


def extract_atom_n_episodes(extract_start_date, extract_end_date) -> tuple[pd.DataFrame, pd.DataFrame]:
    fname = f"{extract_start_date}-{extract_end_date}"

    active_clients_start_date = None  # '2020-01-01'
    active_clients_end_date = None  # '2023-09-15'
    processed_atom_df = extract_prep_atom_data(
        extract_start_date, extract_end_date, active_clients_start_date, active_clients_end_date, fname, purpose='Matching')
    processed_ep_df = extract_prep_episode_data(
        extract_start_date, extract_end_date, f"Matching_eps_{fname}")
    logger.debug(f"extract_atom_n_episodes: Extract Period {fname}")
    logger.debug(f"extract_atom_n_episodes: ATOMs {len(processed_atom_df)}")
    logger.debug(f"extract_atom_n_episodes: Episodes {len(processed_ep_df)}")
    return processed_atom_df, processed_ep_df


# def program_mismatch(ep_df: pd.DataFrame, unmatched_atoms_df: pd.DataFrame) -> pd.DataFrame:
#     unmatched_w_slk_match = unmatched_atoms_df.loc[unmatched_atoms_df['SLK'].isin(
#         ep_df['SLK'])]
#     # Merge on 'SLK'
#     merged_df = pd.merge(unmatched_w_slk_match, ep_df,
#                          on='SLK', suffixes=('_atom', '_ep'))
#     # Find rows where 'Program' values don't match
#     mismatch_df = merged_df[merged_df['Program_atom']
#                             != merged_df['Program_ep']]
# #  program_mismatch_for_matched_slk = mismatch_df[['SLK','Program_atom', 'Program_ep','Staff',	'AssessmentDate','CommencementDate', 'EndDate']].drop_duplicates()
#     program_mismatch_for_matched_slk = mismatch_df[[
#         'SLK', 'Program_atom', 'Program_ep', 'Staff',	'AssessmentDate']].drop_duplicates()
#     return program_mismatch_for_matched_slk


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


def write_audit_results_csv(results, period_str: str):
    # write results to csv
    parent_folder = "data/out/audits"
    for k, v in results.items():        
      v.to_csv(
          f'{parent_folder}/{k}_{period_str}.csv', index=False)
    # results['ProgramMismatch'].to_csv(
    #     f'{parent_folder}/program_mismatch_for_matched_slk_{period_str}.csv', index=False)


def get_unmatched_by_col(ep_df: pd.DataFrame, atoms_df: pd.DataFrame
                         , col_str: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    atoms__noep_for_slk = atoms_df.loc[~atoms_df[col_str].isin(ep_df[col_str])]
    eps__noatom_for_slk = ep_df.loc[~ep_df[col_str].isin(atoms_df[col_str])]

    atoms__noep_for_slk.sort_values(by=col_str, ascending=True, inplace=True)
    eps__noatom_for_slk.sort_values(by=col_str, ascending=True, inplace=True)

    # remove from original dfs
    e = ep_df.loc[~ep_df[col_str].isin(eps__noatom_for_slk['SLK'])]
    a = atoms_df.loc[~atoms_df[col_str].isin(atoms__noep_for_slk['SLK'])]

    return eps__noatom_for_slk, atoms__noep_for_slk, e, a


"""
do all audits  and return results
"""


def do_audits(ep_df, atoms_df):
    # SLK in atom but not in Episodes
    # atoms__noep_for_slk = unmatched_atoms_df.loc[~unmatched_atoms_df['SLK'].isin(
    #     ep_df['SLK'])]
    eps__noatom_for_slk, atoms__noep_for_slk, ep_df, atoms_df = get_unmatched_by_col(
        ep_df, atoms_df, 'SLK')  # sorted
    
    ep_mismatch_slk_prog, atom_mismatch_slk_prog, ep_df, atoms_df = \
      get_lr_mux_unmatched(ep_df, atoms_df, merge_cols=['SLK', 'Program'])

    # program_mismatch_for_matched_slk = program_mismatch(
    #     ep_df, unmatched_atoms_df)

    return {
        'eps__noatom_for_slk': eps__noatom_for_slk,
        'atoms__noep_for_slk': atoms__noep_for_slk,
        'ep_mismatch_slk_prog': ep_mismatch_slk_prog,
        'atom_mismatch_slk_prog': atom_mismatch_slk_prog
        
    }, ep_df, atoms_df
    # return {"NoEpisodeForSLK": atoms__noep_for_slk, "ProgramMismatch": program_mismatch_for_matched_slk}
    # return unmatched_atoms_df


def main(env='local'):
    MyEnvironmentConfig().setup(env)
    extract_start_date = 20230701  # 20230501 #20220701
    extract_end_date = 20231230

    atoms, processed_ep_df = extract_atom_n_episodes(
        extract_start_date, extract_end_date)

    # unmatched_atoms_df, matched = get_matched_unmatched_atoms(
    #     atoms, processed_ep_df)
    audit_results, matched_eps, matched_atoms = do_audits(processed_ep_df, atoms)

    write_audit_results_csv(
        audit_results, f"{extract_start_date}-{extract_end_date}")

    return matched_eps, matched_atoms,  audit_results

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
