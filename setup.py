from setuptools import setup
from os import path

import sys
if len(sys.argv) > 1 and sys.argv[1] == 'test':
      sys.jardin_setup = False
else:
      sys.jardin_setup = True

import jardin

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.rst')) as f:
    long_description = f.read()

setup(
      name = 'jardin',
      version = jardin.__version__,
      description = 'A Pandas dataframe-based ORM',
      long_description = long_description,
      url = 'https://github.com/instacart/jardin',
      author = 'Emmanuel Turlay',
      license = 'MIT',
      author_email = 'emmanuel@instacart.com',
      packages = ['jardin', 'jardin.database', 'jardin.database.drivers'],
      install_requires = [
      'pandas',
      'numpy',
      'psycopg2',
      'memoized_property',
      'inflect',
      'future'
      ],
      python_requires='>=3.5.6, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
      test_suite='tests',
      classifiers = [
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python :: 2',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.5',
      'Programming Language :: Python :: 3.6',
      'Programming Language :: SQL',
      'Topic :: Database',
      'Topic :: Database :: Database Engines/Servers'
      ],
      keywords = 'postgres mysql database ORM'
      )
