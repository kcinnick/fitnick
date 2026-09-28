from fitnick.base.live_api import HealthAPIError
from fitnick_django.fitnick_django import views


class DummyRequest:
    pass


def test_index_suppresses_optional_scope_errors(monkeypatch):
    monkeypatch.setattr('fitnick_django.fitnick_django.views.uses_live_health_api', lambda: True)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_activity_summary', lambda today: {'summary': {'steps': 1234}})

    def scope_error(*args, **kwargs):
        raise HealthAPIError(
            status_code=403,
            provider='google',
            error_type='permission_denied',
            message='Required OAuth scope(s) are missing for this operation.',
        )

    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_identity_summary', scope_error)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_recent_steps', scope_error)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_latest_sleep_session', scope_error)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_latest_body_fat_entry', scope_error)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_health_provider', lambda: 'google')

    captured = {}

    def fake_render(request, template_name, context, status=None):
        captured['template_name'] = template_name
        captured['context'] = context
        captured['status'] = status
        return context

    monkeypatch.setattr('fitnick_django.fitnick_django.views.render', fake_render)

    result = views.index(DummyRequest())

    assert result['steps'] == 1234
    assert result['errors'] == []
    assert result['identity'] is None
    assert result['recent_steps'] == []
    assert result['latest_sleep'] is None
    assert result['latest_body_fat'] is None


def test_index_keeps_non_scope_errors(monkeypatch):
    monkeypatch.setattr('fitnick_django.fitnick_django.views.uses_live_health_api', lambda: True)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_daily_activity_summary', lambda today: {'summary': {'steps': 1234}})
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_identity_summary', lambda: {'provider': 'google'})
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_recent_steps', lambda days=7: [])
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_latest_sleep_session', lambda: None)

    def upstream_error(*args, **kwargs):
        raise HealthAPIError(status_code=500, provider='google', error_type='upstream', message='temporary failure')

    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_latest_body_fat_entry', upstream_error)
    monkeypatch.setattr('fitnick_django.fitnick_django.views.get_health_provider', lambda: 'google')
    monkeypatch.setattr('fitnick_django.fitnick_django.views.render', lambda request, template_name, context, status=None: context)

    result = views.index(DummyRequest())

    assert result['steps'] == 1234
    assert len(result['errors']) == 1
    assert 'temporary failure' in result['errors'][0]

