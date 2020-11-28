import pytest

from fitnick.base.base import introspect_tokens


@pytest.fixture(scope="module", autouse=True)
def validate_tokens():
    valid = introspect_tokens()
    if not valid:
        raise AssertionError
    else:
        print('Token validation passed. Continuing..')
