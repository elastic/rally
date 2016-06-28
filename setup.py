from os.path import join, dirname
from setuptools import setup, find_packages


def str_from_file(name):
    with open(join(dirname(__file__), name)) as f:
        return f.read().strip()


raw_version = str_from_file("version.txt")
VERSION = raw_version.split(".")
__version__ = VERSION
__versionstr__ = raw_version

long_description = str_from_file("README.rst")

install_requires = [
    "elasticsearch==2.3.0",
    "psutil==4.1.0",
    "py-cpuinfo==0.2.3",
    "tabulate==0.7.5",
    "jsonschema==2.5.1",
    "Jinja2==2.8",
    # always use the latest version, these are certificate files...
    "certifi"
]

tests_require = []

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
          exclude=("tests*",)
      ),
      include_package_data=True,
      package_data={"": ["*.json"]},
      install_requires=install_requires,
      test_suite="tests",
      tests_require=tests_require,
      entry_points={
          "console_scripts": ["esrally=esrally.rally:main"],
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
          "Programming Language :: Python :: 3.5"
      ],
      zip_safe=False)
