import os

from django.contrib.auth import get_user_model


def _parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def bootstrap_admin_user():
    """Create or maintain an admin user from env vars for first-boot recovery."""
    if not _parse_bool(os.getenv('FITNICK_BOOTSTRAP_ADMIN_ENABLED'), default=False):
        return {'ok': False, 'action': 'disabled'}

    username = (os.getenv('FITNICK_BOOTSTRAP_ADMIN_USERNAME') or '').strip()
    password = os.getenv('FITNICK_BOOTSTRAP_ADMIN_PASSWORD') or ''
    email = (os.getenv('FITNICK_BOOTSTRAP_ADMIN_EMAIL') or '').strip()
    reset_password = _parse_bool(os.getenv('FITNICK_BOOTSTRAP_ADMIN_RESET_PASSWORD'), default=False)

    if not username or not password:
        print('Bootstrap admin enabled, but username/password env vars are missing.')
        return {'ok': False, 'action': 'missing_credentials'}

    user_model = get_user_model()
    user, created = user_model.objects.get_or_create(
        username=username,
        defaults={
            'email': email,
            'is_staff': True,
            'is_superuser': True,
        },
    )

    if created:
        user.set_password(password)
        setattr(user, 'is_staff', True)
        setattr(user, 'is_superuser', True)
        if email:
            setattr(user, 'email', email)
        user.save()
        print(f'Bootstrap admin user created: {username}')
        return {'ok': True, 'action': 'created', 'username': username}

    changed = False
    if not getattr(user, 'is_staff', False):
        setattr(user, 'is_staff', True)
        changed = True
    if not getattr(user, 'is_superuser', False):
        setattr(user, 'is_superuser', True)
        changed = True
    if email and getattr(user, 'email', '') != email:
        setattr(user, 'email', email)
        changed = True
    if reset_password:
        user.set_password(password)
        changed = True

    if changed:
        user.save()
        print(f'Bootstrap admin user updated: {username}')
        return {'ok': True, 'action': 'updated', 'username': username}

    print(f'Bootstrap admin user already present: {username}')
    return {'ok': True, 'action': 'exists', 'username': username}

