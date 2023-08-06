# as oppose to b.ipynb

import mylogger
from utils.df_xtrct_prep import extract_prep_data
from statsutil.funcs import get_all_results
from utils.io import write_results_to_files, create_results_folder
from filters import get_filters, apply_filters, get_outfilename_for_filters
logger = mylogger.get(__name__)


def do_all(df, chosen_surveys, orig_filter, outfolder="./data/out"):

  outfile_name = get_outfilename_for_filters(orig_filter)
  
  filters = get_filters(orig_filter , exclude_fields=['FunderName'])
  new_df = apply_filters(df, filters)

  all_results = get_all_results(new_df, chosen_surveys, filters)
  
  logger.debug(f"going to write all_results: {all_results} to {outfolder}/{outfile_name}.csv")

  write_results_to_files(all_results, f"{outfolder}/{outfile_name}.csv")

  return all_results


def main ():
  results_folder = "./data/out"
  
  chosen_surveys = [1, 3 ,6]
  extract_start_date = 20200101
  extract_end_date = 20240101

  period_folder = f"{extract_start_date}_{extract_end_date}_1"

  outfolder = f"{results_folder}/{period_folder}"

  active_clients_start_date ='2022-07-01' 
  active_clients_end_date = '2023-06-30'

  logger.info(f"Extracting data from {extract_start_date} to {extract_end_date}")

  # Extract & Process
  processed_df = extract_prep_data(extract_start_date, extract_end_date
                                  , active_clients_start_date
                                  , active_clients_end_date
                                  , period_folder, min_atoms_per_client =3)
  
  # create period folders to prep for results .csv files to be dropped in
  create_results_folder(f"{results_folder}/{period_folder}")

  orig_filter1 = {'FunderName': 'NSW Ministry of Health'}
  MoH_results = do_all(processed_df, chosen_surveys, orig_filter1, outfolder)
  # orig_filter1 = {'FunderName': 'Coordinaire'}
  # Coordinaire_results = do_all(processed_df, chosen_surveys, orig_filter1)

  # orig_filter1 = {'FunderName': 'Murrumbidgee PHN'}
  # MPHN_results = do_all(processed_df, chosen_surveys, orig_filter1)

  # orig_filter1 = {'FunderName': 'ACT Health'}
  # ACTHealth_results = do_all(processed_df, chosen_surveys, orig_filter1)


if __name__ == '__main__':
    # log_tester()
    main()

# logging.basicConfig(level=logging.INFO,
#                         format='%(asctime)s %(levelname)s %(message)s',
#                         datefmt='%Y-%m-%d %H:%M:%S',
#                         filename='logs/app.log',
#     )    

# import logging

# def log_tester() -> None:
#     logger = mylogger.get(__name__)

#     logger.debug("Hello Debug!")
#     logger.info("Hello Info!")
#     logger.warning("Hello Warning!")
#     logger.error("Hello Error!")
#     logger.critical("Hello Critical!")
#     print('Hello World!')