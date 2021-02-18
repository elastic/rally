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
    properties = ["timed_out", "took"]
    if get_point_in_time:
        properties.append("pit_id")
    # we only need to parse these the first time
    if hits_total is None:
        properties.extend(["hits.total", "hits.total.value", "hits.total.relation"])

    parsed = parse(response, properties)

    # standardize these before returning...
    parsed["hits.total.value"] = parsed.pop("hits.total.value", parsed.pop("hits.total", 0))
    parsed["hits.total.relation"] = parsed.get("hits.total.relation", "eq")

    response_str = response.getvalue().decode("UTF-8")
    index_of_last_sort = response_str.rfind('"sort"')
    sort_pattern = r"sort\":([^\]]*])"
    last_sort_str = re.search(sort_pattern, response_str[index_of_last_sort::])
    if last_sort_str is not None:
        last_sort = json.loads(last_sort_str.group(1))
    else:
        last_sort = None
    return parsed, last_sort
