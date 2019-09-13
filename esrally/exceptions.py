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


class RallyError(Exception):
    """
    Base class for all Rally exceptions
    """

    def __init__(self, message, cause=None):
        super().__init__(message, cause)
        self.message = message
        self.cause = cause

    def __repr__(self):
        return self.message


class LaunchError(RallyError):
    """
    Thrown whenever there was a problem launching the benchmark candidate
    """


class SystemSetupError(RallyError):
    """
    Thrown when a user did something wrong, e.g. the metrics store is not started or required software is not installed
    """


class RallyAssertionError(RallyError):
    """
    Thrown when a (precondition) check has been violated.
    """


class DataError(RallyError):
    """
    Thrown when something is wrong with the benchmark data
    """


class SupplyError(RallyError):
    pass


class BuildError(RallyError):
    pass


class InvalidSyntax(RallyError):
    pass


class InvalidName(RallyError):
    pass


class TrackConfigError(RallyError):
    """
    Thrown when something is wrong with the track config e.g. user supplied a track-param
    that can't be set
    """
    pass


class NotFound(RallyError):
    pass
