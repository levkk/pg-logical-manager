'''Test replication slot management.'''
import pytest
import click
from click.testing import CliRunner

# Target
from manager import create_replication_slot, drop_replication_slot, _ensure_connected

def slot_check(name, exists):
    src, dest = _ensure_connected()
    cursor = src.cursor()
    cursor.execute("SELECT count(*) AS count FROM pg_replication_slots WHERE slot_name = %s", (name,))
    count = cursor.fetchone()[0]

    if exists:
        assert count == 1
    else:
        assert count == 0

    src.close()
    dest.close()

def wal_level():
    src, dest = _ensure_connected()
    cursor = src.cursor()

    cursor.execute('SHOW wal_level')

    return cursor.fetchone()[0]


def test_replication_slot():
    if wal_level() != 'logical':
        pytest.skip('Unsupported configuration. wal_level must be logical.')

    runner = CliRunner()

    result = runner.invoke(create_replication_slot, ['test_slot'])
    slot_check('test_slot', True)
    assert result.exit_code == 0

    # Test idempotency
    result = runner.invoke(create_replication_slot, ['test_slot'])
    assert result.exit_code == 0
    assert 'already exists' in result.output

    result = runner.invoke(drop_replication_slot, ['test_slot'])
    slot_check('test_slot', False)
    assert result.exit_code == 0

    # Test idempotency
    result = runner.invoke(drop_replication_slot, ['test_slot'])
    assert result.exit_code == 0
    assert 'does not exist' in result.output
