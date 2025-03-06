# Constants for error keys expected in the data records
import datetime
import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


INSCRIPTION_ERROR_KEYS = ["errorMonotributo", "errorConstancia", "errorRegimenGeneral"]


def save_report_json(
    total_data_len: int, report_title: str, data: Dict[str, Any]
) -> None:
    """
    Saves a report in JSON format with the specified title and data.

    Args:
        total_data_len (int): Total number of persons checked.
        report_title (str): Title of the report.
        data (Dict[str, Any]): Dictionary containing the information to be saved.
    """
    now = datetime.datetime.now()
    formatted_date = now.strftime("%d/%m/%Y %H:%M:%S")
    formatted_filename_date = now.strftime("%d-%m-%Y_%H-%Mhs")

    final_report = {
        "total_persons_checked": total_data_len,
        report_title: {"total": len(data), "nits_list": data},
        "query_date": formatted_date,
    }

    filename = f"persons_total_report_{report_title}_{formatted_filename_date}.json"

    try:
        with open(filename, "w", encoding="utf-8") as json_file:
            json.dump(final_report, json_file, indent=4, ensure_ascii=False)
        logger.info("Final report saved in JSON file: %s", filename)
    except Exception as e:
        logger.error("Error saving report: %s", str(e))


def filter_dictionary(
    dict_to_filter: Dict[Any, Any], keys_to_remove: Dict[Any, Any]
) -> Dict[Any, Any]:
    """
    Returns a new dictionary by filtering out the keys that are present in keys_to_remove.

    Args:
        dict_to_filter (Dict[Any, Any]): Original dictionary to be filtered.
        keys_to_remove (Dict[Any, Any]): Dictionary whose keys should be removed from dict_to_filter.

    Returns:
        Dict[Any, Any]: Filtered dictionary.
    """
    return {k: v for k, v in dict_to_filter.items() if k not in keys_to_remove}
