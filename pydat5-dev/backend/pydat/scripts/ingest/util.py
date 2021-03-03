def merge_nested_dictionaries(d_a, d_b):
    for k, v in d_b.items():
        if isinstance(v, (str, int, float)):  # values overwrite
            d_a[k] = v
        elif isinstance(v, list) and isinstance(d_a.setdefault(k, []), list):  # lists merge
            d_a[k].extend(v)
        elif isinstance(v, dict) and isinstance(d_a.setdefault(k, {}), dict):  # dicts merge
            d_a[k] = merge_nested_dictionaries(d_a[k], v)
        else:
            raise Exception('unsupported type to merge')
    return d_a
