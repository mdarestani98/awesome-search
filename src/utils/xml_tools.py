from typing import Union, Tuple, Dict, Any


def safe_parse(record: Dict[str, Any], keys: Tuple[str, ...]) -> Union[Dict, None]:
    try:
        for key in keys:
            record = record[key]
        return record
    except KeyError:
        return None
