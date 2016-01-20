from setuptools import setup, find_packages

VERSION = (0, 1, 0, 'dev0')
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))

install_requires = [
  "elasticsearch>=2.1.0",
  "psutil>=3.3.0,<4.0.0",
  "certifi"
]

tests_require = []

# we call the tool rally, but it will be published as esrally on pypi
setup(name="esrally",
      version=__versionstr__,
      description="Macrobenchmarking framework for Elasticsearch",
      url="https://github.com/elastic/rally",
      license="Apache License, Version 2.0",
      packages=find_packages(
        where='.',
        exclude=('tests*', )
      ),
      install_requires=install_requires,
      test_suite='tests',
      tests_require=tests_require,
      entry_points={
        "console_scripts": ["esrally=rally.rally:main"],
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
