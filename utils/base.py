
"""
    Merge two dictionaries, excluding keys from the second dictionary.
    Returns a new dictionary with the contents of dict1 and dict2.
    If a key is in both dictionaries, the value from the second dictionary (dict2) is used.
"""
def merge_dicts_exclude_keys(dict1, dict2, exclude_keys):
    return {k: v for k, v in {**dict1, **dict2}.items() if k not in exclude_keys}