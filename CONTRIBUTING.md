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

### Submitting your changes

Once your changes and tests are ready to submit for review:

1. Test your changes

    Run the test suite to make sure that nothing is broken: `python3 setup.py test`.

2. Sign the Contributor License Agreement

    Please make sure you have signed our [Contributor License Agreement](https://www.elastic.co/contributor-agreement/). We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.

3. Rebase your changes

    Update your local repository with the most recent code from the main Rally repository, and rebase your branch on top of the latest master branch. We prefer your initial changes to be squashed into a single commit. Later, if we ask you to make changes, add them as separate commits.  This makes them easier to review.  As a final step before merging we will either ask you to squash all commits yourself or we'll do it for you.


4. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".

Then sit back and wait. There will probably be discussion about the pull request and, if any changes are needed, we would love to work with you to get your pull request merged into Rally.

# Contributing to the Rally codebase

**Repository:** [https://github.com/elastic/rally](https://github.com/elastic/rally)

Please follow the guidelines in the [README](README.rst) on the required software and the setup for development.
