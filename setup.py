from os.path import join, dirname

try:
    from setuptools import setup, find_packages
except ImportError:
    print("*** Could not find setuptools. Did you install pip3? *** \n\n")
    raise


def str_from_file(name):
    with open(join(dirname(__file__), name)) as f:
        return f.read().strip()


raw_version = str_from_file("version.txt")
VERSION = raw_version.split(".")
__version__ = VERSION
__versionstr__ = raw_version

long_description = str_from_file("README.rst")

install_requires = [
    "elasticsearch==5.3.0",
    "psutil==5.4.0",
    "py-cpuinfo==3.2.0",
    "tabulate==0.7.5",
    "jsonschema==2.5.1",
    "Jinja2==2.9.5",
    # remote messaging
    "thespian==3.8.0",
    # recommended library for thespian to identify actors more easily with `ps`
    # "setproctitle==1.1.10",
    # always use the latest version, these are certificate files...
    "certifi"
]

tests_require = [
    "pytest==3.0.6",
    "pytest-benchmark==3.0.0"
]

# we call the tool rally, but it will be published as esrally on pypi
setup(name="esrally",
      maintainer="Daniel Mitterdorfer",
      maintainer_email="daniel.mitterdorfer@gmail.com",
      version=__versionstr__,
      description="Macrobenchmarking framework for Elasticsearch",
      long_description=long_description,
      url="https://github.com/elastic/rally",
      license="Apache License, Version 2.0",
      packages=find_packages(
          where=".",
          exclude=("tests*", "benchmarks*")
      ),
      include_package_data=True,
      package_data={"": ["*.json", "*.yml"]},
      install_requires=install_requires,
      test_suite="tests",
      tests_require=tests_require,
      setup_requires=[
          "pytest-runner==2.10.1",
      ],
      entry_points={
          "console_scripts": [
              "esrally=esrally.rally:main",
              "esrallyd=esrally.rallyd:main"
          ],
      },
      classifiers=[
          "Topic :: System :: Benchmark",
          "Development Status :: 4 - Beta",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Developers",
          "Operating System :: MacOS :: MacOS X",
          "Operating System :: POSIX",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.4",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6"
      ],
      zip_safe=False)
