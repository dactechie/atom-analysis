import numpy as np

def range_average(range_str:str, separator:str):
  two_ints = range_str.split(separator)
  return (int(two_ints[0])+int(two_ints[1]))/2



def limit_pkey_num_assessments(df, min_num_assessments):

    g = df.groupby('PartitionKey')
    counts =  g['PartitionKey'].count()
    gt2_ATOM_SLKs = counts[ counts >= min_num_assessments ].index.tolist()
    df_gt2 = df [ df['PartitionKey'].isin(gt2_ATOM_SLKs) ]

    return df_gt2


def get_clean_for_col(df, col:str):# ->pd.DataFrame:

  dfsds = df[ ~df[col].isna() ]       # exclude rows with NaN
  g = dfsds.groupby('PartitionKey')

  not_all_zeros_df = g.filter(lambda x: any(x[col]) > 0)  # only those PartitionKeys who have at least one > 0 value
  
  return not_all_zeros_df


def first_n_medians(g, col:str, min_atoms: int):
  nth_medians = [np.median(g.nth(i).loc[:, col]) for i in range(min_atoms) ]
  return nth_medians
    
    
# grp.get_group('YKSLA240819972')
# grp.nth(1)['SDS_Score']
# grp.first()


# # Limit Client by Number of ATOMs done
# def limit_by_num_atoms(df, num_adoms=5, gt_or_eq='>='):
#   if gt_or_eq == '>':
#     fn = lambda x: len(x) > num_adoms
#   elif gt_or_eq == '=':
#     fn = lambda x: len(x) == num_adoms
#   else:
#     fn = lambda x: len(x) >= num_adoms
  
#   #https://stackoverflow.com/questions/17109419/pandas-filtering-pivot-table-rows-where-count-is-fewer-than-specified-value
#   return  df[df.groupby('PartitionKey')['PartitionKey'].transform(fn).astype('bool')].reset_index(drop=True)    