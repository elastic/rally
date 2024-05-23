# Contributing to Rally

Rally is an open source project and we love to receive contributions from our community — you! There are many ways to contribute, from
writing tutorials or blog posts, improving the documentation, submitting bug reports and feature requests or writing code which can be 
incorporated into Rally itself.

## Bug reports

If you think you have found a bug in Rally, first make sure that you are testing against 
the [latest version of Rally](https://github.com/elastic/rally/releases) - your issue may already have been fixed. If not, search our 
[issues list](https://github.com/elastic/rally/issues) on GitHub in case a similar issue has already been opened.

It is very helpful if you can prepare a reproduction of the bug. In other words, provide a small test case which we can run to confirm 
your bug. It makes it easier to find the problem and to fix it.

## Feature requests

If you find yourself wishing for a feature that doesn't exist in Rally, you are probably not alone. There are bound to be others out there 
with similar needs. Open an issue on our [issues list](https://github.com/elastic/rally/issues) on GitHub which describes the feature you 
would like to see, why you need it, and how it should work.

## Contributing code, benchmarks and documentation changes

If you have a bugfix or new feature that you would like to contribute to Rally, please find or open an issue about it first. Talk about 
what you would like to do. It may be that somebody is already working on it, or that there are particular issues that you should know about
before implementing the change.

We enjoy working with contributors to get their code accepted. There are many approaches to fixing a problem and it is important to find 
the best approach before writing too much code.

The process for contributing to any of the [Elastic repositories](https://github.com/elastic/) is similar. Details for individual projects 
can be found below.

If you want to get started in the project, a good idea is to check issues labeled with [help wanted](https://github.com/elastic/rally/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22). These are issues that should help newcomers to the project get something achieved without too much hassle. 

### Fork and clone the repository

You will need to fork the Rally repository and clone it to your local machine. See 
the [Github help page](https://help.github.com/articles/fork-a-repo) for help.

### Importing the project into IntelliJ IDEA

Rally builds using virtualenv. When importing into IntelliJ you will need to define an appropriate Python SDK, which is provided by virtualenv.
For more details on defining a Python SDK in IntelliJ please refer to their documentation. We recommend using the Python SDK that `make prereq` creates.
This is typically created via `Virtualenv Environment` / `Existing Environment` and pointing to `.venv/bin/python3` within the Rally source directory.

In order to run tests within the IDE, ensure the `Python Integrated Tools` / `Testing` / `Default Test Runner` is set to `pytest`.

### Submitting your changes

Once your changes and tests are ready to submit for review:

1. Format your changes

    Make sure that lint is passing by running `make lint`. To format your code, run `make format`.

    Consider using editor integrations to do it automatically: you'll need to configure [black](https://black.readthedocs.io/en/stable/integrations/editors.html) and [isort](https://github.com/PyCQA/isort/wiki/isort-Plugins).

2. Test your changes

    Ensure that all tests pass by running `make check-all`. This runs sequentially lint checks, unit tests and integration tests. These can be executed in isolation using `make lint`, `make test` and `make it` respectively, in case you need to iterate over a subset of tests.

    Note: Integration tests are much slower than unit tests.

3. Sign the Contributor License Agreement

    Please make sure you have signed our [Contributor License Agreement](https://www.elastic.co/contributor-agreement/). We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.

4. Rebase your changes

    Update your local repository with the most recent code from the main Rally repository, and rebase your branch on top of the latest master branch. We prefer your initial changes to be squashed into a single commit. Later, if we ask you to make changes, add them as separate commits.  This makes them easier to review.  As a final step before merging we will either ask you to squash all commits yourself or we'll do it for you.


5. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".

Then sit back and wait. There will probably be discussion about the pull request and, if any changes are needed, we would love to work with you to get your pull request merged into Rally.

Note: Contributors belonging to the "Elastic" organization on Github can merge PRs themselves after getting a "LGTM" (Looks good to me); this workflow is similar to the established one in the Elasticsearch project.

# Contributing to the Rally codebase

**Repository:** [https://github.com/elastic/rally](https://github.com/elastic/rally)

Please follow the guidelines in the [README](README.md) on the required software and the setup for development.

## License Headers

We require the following license header on all source code files:

```
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
``` 
