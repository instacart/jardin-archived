from setuptools import setup
from os import path

import jardin

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.rst')) as f:
    long_description = f.read()

setup(name = 'jardin',
      version = jardin.__version__,
      description = 'A dataframe-base ORM',
      long_description = long_description,
      url = 'https://github.com/instacart/jardin',
      author = 'Emmanuel Turlay',
      license = 'MIT',
      author_email = 'emmanuel@instacart.com',
      packages = ['jardin'],
      install_requires = [
      'pandas',
      'numpy',
      'psycopg2',
      'memoized_property'
      ],
      python_requires='>=2.7, <3',
      classifiers = [
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python :: 2.7',
      ],
      keywords = 'postgres database ORM')