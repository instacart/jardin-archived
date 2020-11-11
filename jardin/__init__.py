import sys

if not hasattr(sys, 'jardin_setup') or (hasattr(sys, 'jardin_setup') and not sys.jardin_setup):
    from jardin.model import Model, Collection
    from jardin.query import query
    from jardin.transaction import Transaction
    from jardin.tools import reset_session
    import jardin.config

__author__ = 'Emmanuel Turlay'
__copyright__ = 'Copyright 2017, Instacart'
__credits__ = ['Emmanuel Turlay', 'Mathieu Ripert']
__license__ = 'MIT'
__version__ = '0.20.0'
__maintainer__ = 'Emmanuel Turlay'
__email__ = 'emmanuel@instacart.com'
__status__ = 'Prototype'
