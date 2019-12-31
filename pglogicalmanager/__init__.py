'''PostgreSQL logical replication manager.'''
from .manager import *
from .manager import _ensure_connected, _memberof, _superuser # Handy for tests.
