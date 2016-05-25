from os.path import join, dirname
from setuptools import setup, find_packages

VERSION = (0, 3, 0, "dev")
__version__ = VERSION
__versionstr__ = ".".join(map(str, VERSION))

f = open(join(dirname(__file__), "README.rst"))
long_description = f.read().strip()
f.close()

install_requires = [
    "elasticsearch==2.3.0",
    "psutil==4.1.0",
    "py-cpuinfo==0.2.3",
    "tabulate==0.7.5",
    "jsonschema==2.5.1",
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
