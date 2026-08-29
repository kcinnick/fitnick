import fitnick_django.fitnick_django.bootstrap_admin as bootstrap


class FakeUser:
    def __init__(self, username='nick', email='', is_staff=False, is_superuser=False):
        self.username = username
        self.email = email
        self.is_staff = is_staff
        self.is_superuser = is_superuser
        self.password_set = None
        self.save_calls = 0

    def set_password(self, raw_password):
        self.password_set = raw_password

    def save(self):
        self.save_calls += 1


class FakeManager:
    def __init__(self, user=None, created=False):
        self._user = user or FakeUser()
        self._created = created

    def get_or_create(self, username, defaults):
        if self._created:
            user = FakeUser(
                username=username,
                email=defaults.get('email', ''),
                is_staff=defaults.get('is_staff', False),
                is_superuser=defaults.get('is_superuser', False),
            )
            return user, True
        return self._user, False


class FakeUserModel:
    def __init__(self, manager):
        self.objects = manager


def test_bootstrap_disabled(monkeypatch):
    monkeypatch.delenv('FITNICK_BOOTSTRAP_ADMIN_ENABLED', raising=False)
    result = bootstrap.bootstrap_admin_user()
    assert result['action'] == 'disabled'


def test_bootstrap_missing_credentials(monkeypatch):
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_ENABLED', '1')
    monkeypatch.delenv('FITNICK_BOOTSTRAP_ADMIN_USERNAME', raising=False)
    monkeypatch.delenv('FITNICK_BOOTSTRAP_ADMIN_PASSWORD', raising=False)
    result = bootstrap.bootstrap_admin_user()
    assert result['action'] == 'missing_credentials'


def test_bootstrap_creates_user(monkeypatch):
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_ENABLED', '1')
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_PASSWORD', 'secret')
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_EMAIL', 'admin@example.com')
    monkeypatch.setattr(
        'fitnick_django.fitnick_django.bootstrap_admin.get_user_model',
        lambda: FakeUserModel(FakeManager(created=True)),
    )

    result = bootstrap.bootstrap_admin_user()
    assert result['action'] == 'created'
    assert result['username'] == 'admin'


def test_bootstrap_existing_user_reset_password(monkeypatch):
    existing = FakeUser(username='admin', email='a@example.com', is_staff=True, is_superuser=True)
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_ENABLED', '1')
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_PASSWORD', 'rotated')
    monkeypatch.setenv('FITNICK_BOOTSTRAP_ADMIN_RESET_PASSWORD', '1')
    monkeypatch.setattr(
        'fitnick_django.fitnick_django.bootstrap_admin.get_user_model',
        lambda: FakeUserModel(FakeManager(user=existing, created=False)),
    )

    result = bootstrap.bootstrap_admin_user()
    assert result['action'] == 'updated'
    assert existing.password_set == 'rotated'

