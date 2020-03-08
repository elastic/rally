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
