import pytest

from fitnick.base.base import introspect_tokens


@pytest.fixture(scope="module", autouse=True)
def validate_tokens():
    valid = introspect_tokens()
    if not valid:
        pytest.exit(msg='Token validation failed. Please refresh tokens before running tests!')
    else:
        print('Token validation passed. Continuing..')
