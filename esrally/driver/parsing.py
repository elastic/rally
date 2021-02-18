import json
import re
from typing import List
from io import BytesIO

import ijson


def parse(text: BytesIO, props: List[str], lists: List[str] = None):
    """
    Selectively parsed the provided text as JSON extracting only the properties provided in ``props``. If ``lists`` is
    specified, this function determines whether the provided lists are empty (respective value will be ``True``) or
    contain elements (respective key will be ``False``).

    :param text: A text to parse.
    :param props: A mandatory list of property paths (separated by a dot character) for which to extract values.
    :param lists: An optional list of property paths to JSON lists in the provided text.
    :return: A dict containing all properties and lists that have been found in the provided text.
    """
    text.seek(0)
    parser = ijson.parse(text)
    parsed = {}
    parsed_lists = {}
    current_list = None
    expect_end_array = False
    try:
        for prefix, event, value in parser:
            if expect_end_array:
                # True if the list is empty, False otherwise
                parsed_lists[current_list] = event == "end_array"
                expect_end_array = False
            if prefix in props:
                parsed[prefix] = value
            elif lists is not None and prefix in lists and event == "start_array":
                current_list = prefix
                expect_end_array = True
            # found all necessary properties
            if len(parsed) == len(props) and (lists is None or len(parsed_lists) == len(lists)):
                break
    except ijson.IncompleteJSONError:
        # did not find all properties
        pass

    parsed.update(parsed_lists)
    return parsed


def extract_search_after_properties(response: BytesIO, get_point_in_time: bool, hits_total):
    response_str = response.getvalue().decode("UTF-8")
    if get_point_in_time:
        pit_id_pattern = r'"pit_id": ?"([^"]*)'
        result = re.search(pit_id_pattern, response_str)
        pit_id = result.group(1)
    else:
        pit_id = None

    # we only need to parse this the first time
    if hits_total is None:
        parsed_hits = parse(response, ["hits.total", "hits.total.value"])
        hits_total = parsed_hits.get("hits.total.value", parsed_hits.get("hits.total", 0))

    index_of_last_sort = response_str.rfind('"sort"')
    sort_pattern = r"sort\":([^\]]*])"
    last_sort_str = re.search(sort_pattern, response_str[index_of_last_sort::])
    if last_sort_str is not None:
        last_sort = json.loads(last_sort_str.group(1))
    else:
        last_sort = None
    return pit_id, last_sort, hits_total
