
import logging
import pandas as pd

def get_mask_datefit(row, slack_days:int = 0):
    # Convert to datetime if not already in that format
    # assessment_date = pd.to_datetime(row['AssessmentDate'], errors='coerce')
    # commencement_date = pd.to_datetime(row['CommencementDate'], errors='coerce')
    # end_date = pd.to_datetime(row['EndDate'], errors='coerce')

    # Create a Timedelta for slack days
    slack_td = pd.Timedelta(days=slack_days)
  
    # Check conditions
    after_commencement = pd.Timestamp(row['AssessmentDate']) >= (row['CommencementDate'] - slack_td)
    before_end_date = pd.Timestamp(row['AssessmentDate']) <= (row['EndDate'] + slack_td)

    return after_commencement and before_end_date


def match_assessments(episodes_df, atoms_df, matching_ndays_slack: int=0):

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

def check_atom_in_multi_episodes(matched_df:pd.DataFrame,slack_ndays:int):
  
  g = matched_df.groupby('SLK_RowKey')['SLK_RowKey'].nunique()
  duplicates = g[g>1]
  duplicate_rows_df = None
  if len(duplicates) > 0:  
      # Get the keys for the duplicate rows
    duplicate_keys = duplicates.index

    # Filter matched_df to show only rows that match the duplicate keys
    duplicate_rows_df = matched_df[matched_df.set_index(['SLK', 'RowKey']).index.isin(duplicate_keys)]
    logging.warn(f"ATOM has matches in multiple episodes {duplicate_rows_df}")
    logging.warn(f"Duplicate matches for slack : {slack_ndays}", duplicate_rows_df)
  return duplicate_rows_df
  


def match_increasing_slack(ep_df, atom_df, max_slack:int=7):
  matching_ndays_slack = 0 
  unmatched_atoms = atom_df
  result_matched_dfs = []
  
  while matching_ndays_slack <= max_slack:
      # Get matched assessments with the current slack
      
      matched_df = match_assessments(ep_df, unmatched_atoms, matching_ndays_slack)
      duplicate_rows_df = check_atom_in_multi_episodes(matched_df, matching_ndays_slack)

      if len(matched_df) == 0:
          result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
          return result_matched_df, None
      
      # Add the matched DataFrame to the list
      result_matched_dfs.append(matched_df)

      # Update unmatched_atoms by filtering out matched SLKs from the current unmatched_atoms
      # matched_slks = matched_df.SLK.unique()
      unmatched_atoms = unmatched_atoms[~unmatched_atoms.SLK_RowKey.isin(matched_df.SLK_RowKey)]
      # there may be other assessments for this SLK that can match if the slack dways are increased
      # don't exclude the SLK, but the SLK +RowKey

      # Increment the slack days for the next iteration
      matching_ndays_slack += 1

  if len(unmatched_atoms) > 0 :
     print(f"There are still {len(unmatched_atoms)} unmatched ATOMs")
     print(f"Unmatched by program: {len(unmatched_atoms.Program.value_counts())}")
    #  logger.info(f"There are still {len(unmatched_atoms)} unmatched ATOMs")
    #  logger.info(f"Unmatched by program: {len(unmatched_atoms.Program.value_counts())}")

  # Concatenate all matched DataFrames from the list
  result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
  return result_matched_df, unmatched_atoms