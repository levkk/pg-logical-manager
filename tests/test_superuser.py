'''Test role membershup and superuser checks.'''
import pytest

from pglogicalmanager import _memberof, _ensure_connected

@pytest.fixture
def conn():
    src, _ = _ensure_connected()

    src.rollback()
    src.set_session(autocommit=False)

    return src


def test_rds_superuser(conn):
    '''User is RDS superuser.'''
    cursor = conn.cursor()

    cursor.execute('CREATE ROLE rds_superuser')
    cursor.execute('GRANT rds_superuser TO CURRENT_USER')

    assert _memberof(conn, role='rds_superuser')

    conn.rollback()

def test_not_rds_superuser(conn):
    '''User is not RDS superuser.'''
    cursor = conn.cursor()

    cursor.execute('CREATE ROLE rds_superuser')

    # But no grant...so sad

    assert not _memberof(conn, role='rds_superuser')

def test_no_such_role(conn):
    '''We are not on RDS, so no rds_superuser role present.'''
    cursor = conn.cursor()

    assert not _memberof(conn, role='rds_superuser')
