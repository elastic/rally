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

set -eo pipefail
export PATH="$HOME/.pyenv/bin:$PATH"
export TERM=dumb
export LC_ALL=en_US.UTF-8
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
make prereq
make install
make precommit
make it

# this will only be done if the build number variable is present
if [ ! -z "$BUILD_NUMBER" ] && [ -d ".rally" ]; then
  tar -cvjf .rally/$BUILD_NUMBER.tar.bzip $( find .rally/ -name "*.log" )
fi
