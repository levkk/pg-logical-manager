'''Test subscription management.'''
import pytest
import click
from click.testing import CliRunner

# Target
from manager import list_subscriptions, _ensure_connected

def test_list_subscriptions():
    runner = CliRunner()

    result = runner.invoke(list_subscriptions)

    assert 'No subscriptions found' in result.output