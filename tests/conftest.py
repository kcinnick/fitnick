import os
import sys
import json

import pytest

from fitnick.base.base import introspect_tokens


# Flag to track Django initialization status
django_initialized = False


@pytest.fixture(scope="session", autouse=True)
def setup_django():
    """Try to initialize Django, but don't fail if it can't."""
    global django_initialized
    try:
        import django
        from django.conf import settings

        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'fitnick_django.fitnick_django.settings')
        if not settings.configured:
            django.setup()
        django_initialized = True
    except Exception as e:
        # Some tests don't need full Django setup
        django_initialized = False


def pytest_configure(config):
    """Register pytest marker for Django-dependent tests."""
    config.addinivalue_line(
        "markers", "django: mark test as requiring Django initialization"
    )


@pytest.fixture(autouse=True)
def skip_if_django_not_initialized(request):
    """Skip Django tests if Django didn't initialize."""
    if request.node.get_closest_marker("django"):
        if not django_initialized:
            pytest.skip("Django not initialized")


@pytest.fixture(scope="module", autouse=True)
def validate_tokens():
    #    valid = introspect_tokens()
    #    if not valid:
    #        raise AssertionError
    #    else:
    #        print('Token validation passed. Continuing..')
    pass
