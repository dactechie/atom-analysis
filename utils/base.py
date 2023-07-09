

def merge_dicts_exclude_keys(dict1, dict2, exclude_keys):
    return {k: v for k, v in {**dict1, **dict2}.items() if k not in exclude_keys}