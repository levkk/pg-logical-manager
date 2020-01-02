'''Test subscription management.'''
import pytest
import click
from click.testing import CliRunner

# Target
from pglogicalmanager import list_subscriptions, create_subscription, drop_subscription, create_replication_slot, list_replication_slots, _ensure_connected

def test_list_subscriptions():
    runner = CliRunner()

    result = runner.invoke(list_subscriptions)

    assert 'No subscriptions found' in result.output


def test_create_subscriptions():
    runner = CliRunner()

    result = runner.invoke(create_subscription, ['test_sub'])

    assert result.exit_code == 0

    result = runner.invoke(list_subscriptions)

    assert 'test_sub' in result.output

    result = runner.invoke(drop_subscription, ['test_sub'])

    assert result.exit_code == 0

    result = runner.invoke(list_subscriptions)

    assert 'No subscriptions found' in result.output


def test_create_subscription_with_replication_slot():
    runner = CliRunner()

    result = runner.invoke(create_replication_slot, ['test_slot_123'])

    assert result.exit_code == 0

    result = runner.invoke(create_subscription, ['test_sub', '--replication-slot=test_slot_123'])

    assert result.exit_code == 0

    result = runner.invoke(list_subscriptions)

    assert 'test_slot_123' in result.output

    result = runner.invoke(drop_subscription, ['test_sub'])

    result = runner.invoke(list_subscriptions)

    assert 'No subscriptions found' in result.output
