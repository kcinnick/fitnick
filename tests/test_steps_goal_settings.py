from fitnick_django.fitnick_django import views


class DummyRequest:
    def __init__(self, method='GET', post_data=None):
        self.method = method
        self.POST = post_data or {}


def test_resolve_steps_goal_prefers_override(monkeypatch, tmp_path):
    settings_file = tmp_path / 'settings.json'
    monkeypatch.setenv('FITNICK_SETTINGS_FILE', str(settings_file))
    views._set_steps_goal_override(7777)

    goal = views._resolve_steps_goal({'goals': {'steps': 12000}})

    assert goal == 7777


def test_resolve_steps_goal_uses_provider_goal(monkeypatch, tmp_path):
    settings_file = tmp_path / 'settings.json'
    monkeypatch.setenv('FITNICK_SETTINGS_FILE', str(settings_file))
    views._clear_steps_goal_override()

    goal = views._resolve_steps_goal({'goals': {'steps': 9100}})

    assert goal == 9100


def test_resolve_steps_goal_uses_default_when_no_override_or_provider(monkeypatch, tmp_path):
    settings_file = tmp_path / 'settings.json'
    monkeypatch.setenv('FITNICK_SETTINGS_FILE', str(settings_file))
    monkeypatch.setenv('FITNICK_DEFAULT_STEPS_GOAL', '13579')
    views._clear_steps_goal_override()

    goal = views._resolve_steps_goal({'summary': {'steps': 4000}})

    assert goal == 13579


def test_settings_page_saves_override(monkeypatch, tmp_path):
    settings_file = tmp_path / 'settings.json'
    monkeypatch.setenv('FITNICK_SETTINGS_FILE', str(settings_file))
    monkeypatch.setattr('fitnick_django.fitnick_django.views.uses_live_health_api', lambda: False)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_health_provider', lambda: 'google')
    monkeypatch.setattr('fitnick_django.fitnick_django.views.render', lambda request, template_name, context: context)

    result = views.settings_page(DummyRequest(method='POST', post_data={'action': 'save_override', 'steps_goal': '8888'}))

    assert result['override_goal'] == 8888
    assert result['effective_goal'] == 8888
    assert result['message']


def test_index_uses_goal_from_provider_response(monkeypatch, tmp_path):
    settings_file = tmp_path / 'settings.json'
    monkeypatch.setenv('FITNICK_SETTINGS_FILE', str(settings_file))
    views._clear_steps_goal_override()

    monkeypatch.setattr('fitnick_django.fitnick_django.views.uses_live_health_api', lambda: True)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_activity_summary', lambda today: {'summary': {'steps': 3210}, 'goals': {'steps': 9000}})
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_identity_summary', lambda: None)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_recent_steps', lambda days=7: [])
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_latest_sleep_session', lambda: None)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_latest_body_fat_entry', lambda: None)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_health_provider', lambda: 'fitbit')
    monkeypatch.setattr('fitnick_django.fitnick_django.views.render', lambda request, template_name, context: context)

    result = views.index(DummyRequest())

    assert result['steps'] == 3210
    assert result['goal'] == 9000

