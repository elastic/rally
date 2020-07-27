# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import json
import os
import re
import sys

__CAMEL_CASE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")


def run_async(t):
    """
    A wrapper that ensures that a test is run in an asyncio context.

    :param t: The test case to wrap.
    """
    def async_wrapper(*args, **kwargs):
        asyncio.run(t(*args, **kwargs), debug=True)
    return async_wrapper


def as_future(result=None, exception=None):
    """

    Helper to create a future that completes immediately either with a result or exceptionally.

    :param result: Regular result.
    :param exception: Exceptional result.
    :return: The corresponding future.
    """
    f = asyncio.Future()
    if exception and result:
        raise AssertionError("Specify a result or an exception but not both")
    if exception:
        f.set_exception(exception)
    else:
        f.set_result(result)
    return f


def load_test_resource(name=None):
    """

    Loads a JSON file containing e.g. mocked Elasticsearch responses from a file. The path to that file is based on the
    following convention:

    ``$TEST_ROOT_PATH/resources/$MODULE_NAME/$TEST_CLASS_NAME/$TEST_METHOD_NAME.json``

    Where:

    * ``$TEST_ROOT_PATH`` is the root path for all tests.
    * ``$MODULE_NAME`` is the full name of the module where the test is implemented excluding the leading "tests".
    * ``$TEST_CLASS_NAME`` is the name of the test class but snake-cased (instead of camel-cased).
    * ``$TEST_METHOD_NAME`` is the name of the test method.

    Example: If we want to load test resources for a test in
             ``tests.driver.runner_test.SelectiveJsonParserTests.test_list_length`` we need to place the file in
             ``$TEST_ROOT_PATH/resources/driver/runner_test/selective_json_parser_tests/test_list_length.json``.

    Note: This method needs to be called directly from the test in which it is used as it works by inspecting the
          caller stack.

    :param name: An optional suffix of the test resource. If a suffix is specified the file name
                 is ``$FILE_$name.json`` instead of ``$FILE.json``.
    :return: A dict representation of the respective file.
    """
    prior_frame = sys._getframe(1)
    # TODO: This assumes that it is always called in the context of a class.
    test_class = prior_frame.f_locals["self"].__class__
    # strip leading "tests" from the module name
    test_module = test_class.__module__[len("tests"):]
    class_name = test_class.__name__
    test_method_name = prior_frame.f_code.co_name

    snake_cased_class_name = __CAMEL_CASE_PATTERN.sub("_", class_name).lower()

    cwd = os.path.dirname(__file__)
    if name is None:
        resource_file_name = f"{test_method_name}.json"
    else:
        resource_file_name = f"{test_method_name}_{name}.json"
    path = os.path.join(cwd, "resources", *test_module.split("."), snake_cased_class_name, resource_file_name)
    with open(path) as f:
        return json.load(f)
