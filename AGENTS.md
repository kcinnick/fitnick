# fitnick agent guide

## Active architecture

The deployed web application is **Django**, not the legacy Flask app.
`render.yaml` runs migrations and Gunicorn against
`fitnick_django.wsgi:application` from the `fitnick_django` directory.

- `fitnick_django\fitnick_django\urls.py`: active routes.
- `fitnick_django\fitnick_django\views.py`: dashboard, goal settings, daily APIs.
- `fitnick_django\fitnick_django\middleware.py`: session/header access control.
- `fitnick_django\fitnick_django\settings.py`: Django configuration and env loading.
- `fitnick\base\live_api.py`: current Google/Fitbit provider access and normalization.
- `fitnick\integrations\bcounter.py`: latest sleep end -> bCounter wake-time sync;
  invoked by `sync_bcounter_wake_time.py`.
- `fitnick\activity`, `fitnick\apis`, and `fitnick\flask_app`: older workflows;
  do not assume they are called by the deployed Django UI.

## Contracts and invariants

- `FITNICK_HEALTH_PROVIDER` selects `google` or `fitbit`; the committed Render
  blueprint selects Google. Preserve both providers when changing shared code.
- Daily APIs: `/api/steps/daily`, `/api/goals/progress`, `/api/sleep/daily`,
  `/api/heart-rate/daily`. They use `from`/`to` inclusive dates, a maximum
  90-day window, and a `{"days": [...]}` envelope. Inspect per-metric null/omission
  behavior rather than assuming every provider supplies every metric.
- Reuse provider helpers, goal resolution, and date-window validation.
  Missing data is not evidence of inactivity; do not silently reinterpret
  missing upstream fields as observed zero in new analysis.
- `FITNICK_TIME_ZONE` defaults to `America/New_York`. Historical final totals
  need to account for civil-day boundaries and late device/provider sync.
- Auth is enforced when `FITNICK_REQUIRE_AUTH` is enabled: Django sessions,
  `X-API-Key`, or configured HTTP Basic credentials. See middleware for exemptions.
- Django uses SQLite; `FITNICK_DJANGO_DB_PATH` controls its location. Render uses
  a persistent disk. Do not delete/reset database files as cleanup.
- Provider capability must be verified before promising device controls such as
  GPS toggling. Data-read access does not imply hardware-control access.
- Prefer `nsorchestrator` for new activity/usage joins; keep domain ownership here
  and preserve the existing direct bCounter wake-time integration.

## Local work and validation

Run tests from this repo root, using its existing virtual environment and the
repository-default import layout. Do not put the nested Django project directory
on `PYTHONPATH` for pytest: tests import `fitnick_django.fitnick_django`, which
would be shadowed by the inner package.

```powershell
.\.venv\Scripts\python.exe -m pytest tests -q
```

Start with relevant test files such as `tests\test_daily_steps_api.py` or
`tests\test_dashboard_scope_handling.py`. `tests\conftest.py` can skip
Django-marked tests if initialization fails: skipped coverage is not a pass.
Mock provider responses and isolate settings files; do not refresh real tokens
or run the wake-time push script as a test.

Local server, when explicitly needed:

```powershell
.\.venv\Scripts\python.exe fitnick_django\manage.py runserver 127.0.0.1:8002
```

Use `render.yaml` for production dependencies and entry-point details.
`setup.py` alone does not list all of the deployed Django stack.

## Workflow

Inspect the branch/worktree; use topic branches, not `main`/`master`. Preserve
unrelated edits. Do not commit, push, deploy, mutate provider data, or run live
syncs unless requested. Keep credentials and personal health data out of docs,
issues, fixtures, and commits. Use PowerShell, Windows paths, `git --no-pager`,
and `curl.exe`. Keep session notes outside the repo and coding guidance here.
