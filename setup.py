from setuptools import setup, find_packages

# we call the tool rally, but it will be published as esrally on pypi
setup(name="esrally",
      version="0.0.1",
      description="Macrobenchmarking framework for Elasticsearch",
      url="https://github.com/elastic/rally",
      license="Apache License, Version 2.0",
      packages=find_packages(),
      install_requires=[
        "elasticsearch>=2.1.0",
        "psutil>=3.3.0,<4.0.0"
      ],
      entry_points = {
        "console_scripts": ["esrally=rally.rally:main"],
      },
      classifiers = [
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
