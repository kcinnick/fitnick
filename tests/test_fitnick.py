#!/usr/bin/env python

"""Tests for `fitnick` package."""

import pytest

from click.testing import CliRunner
import fitbit

from fitnick import fitnick


def test_get_authed_client():
    """Tests that authorization is working as expected."""
    assert type(fitnick.get_authed_client()) == fitbit.Fitbit
