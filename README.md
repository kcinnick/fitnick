# fitnick

Personal fitness dashboard and daily health-data APIs, using Google Health or
Fitbit. The active web application is **Django**. Older Flask, Spark, and CLI
experiments remain in the repository but are not the deployed dashboard.

Part of [core-services](https://github.com/kcinnick/core-services). See
[AGENTS.md](AGENTS.md) for contributor guidance and
[render.yaml](render.yaml) for the production blueprint.

## What works today

- `/`: steps dashboard, goal progress/pacing, and provider-dependent health data.
- `/settings`: set or clear a daily step-goal override. Google Health does not
  expose a daily step goal; use the override or configured default.
- Daily steps, goal progress, sleep, and heart-rate APIs for integrations.
- Historical daily totals are fetched again on refresh, allowing late provider
  corrections after midnight rather than freezing the last pre-midnight total.
- Optional sleep-end-to-bCounter wake-time sync.

Provider read access does **not** imply device control: remote GPS/location
toggling is not implemented. Missing metrics or omitted days do not establish
inactivity, and this dashboard does not make medical or causal claims.

## Local setup (PowerShell)

Use Python 3.11 or newer; the Render blueprint currently uses Python 3.11.
Run these commands from the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e . "Django>=4.2,<5.1" "gunicorn>=23,<24"
Copy-Item .env.example .env
```

Edit `.env` with your own configuration. Never commit tokens, passwords, or
database files. The Django settings loader reads the root `.env` (or, if absent,
`fitnick_django\.env`); process environment variables take precedence.

For an isolated UI preview, set `FITNICK_OFFLINE_MODE=1`. Offline/demo data is not
real provider activity. For live data, set `FITNICK_OFFLINE_MODE=0` and configure
the chosen provider's OAuth credentials.

```powershell
.\.venv\Scripts\python.exe fitnick_django\manage.py migrate
.\.venv\Scripts\python.exe fitnick_django\manage.py createsuperuser
.\.venv\Scripts\python.exe fitnick_django\manage.py runserver 127.0.0.1:8002
```

Open `http://127.0.0.1:8002/login`, then `/`. The example enables authentication;
create a Django user rather than disabling authentication on a publicly reachable
server. Migrations affect the configured database: check
`FITNICK_DJANGO_DB_PATH` before running them against an existing installation.

## Configuration

Start from [.env.example](.env.example), not hard-coded credentials.

| Setting | Purpose |
|---|---|
| `FITNICK_HEALTH_PROVIDER` | `google` (default) or `fitbit` |
| `FITNICK_OFFLINE_MODE` | `1` for offline mode; `0` for live provider reads |
| `FITNICK_TIME_ZONE` | Civil-day/timezone handling; defaults to `America/New_York` |
| `GOOGLE_HEALTH_ACCESS_TOKEN`, `GOOGLE_HEALTH_REFRESH_TOKEN`, `GOOGLE_HEALTH_CLIENT_ID`, `GOOGLE_HEALTH_CLIENT_SECRET` | Google Health OAuth credentials |
| `FITBIT_ACCESS_TOKEN`, `FITBIT_REFRESH_TOKEN`, `FITBIT_AUTH_HEADER` | Fitbit credentials; auth header is the Base64 client-id/client-secret pair |
| `FITNICK_AUTO_REFRESH_TOKENS`, `FITNICK_PROACTIVE_REFRESH_TOKENS` | Automatic/reactive and proactive token refresh |
| `FITNICK_DEFAULT_STEPS_GOAL` | Default goal if no UI override/provider goal is available |
| `FITNICK_ACTIVE_END_HOUR` | Local walking-day end hour used for pacing |
| `FITNICK_SETTINGS_FILE` | Persistent JSON file for UI goal overrides |
| `FITNICK_REQUIRE_AUTH` | Enable Django-session or configured header authentication |
| `FITNICK_API_KEY` | API callers send this in `X-API-Key` |
| `FITNICK_BASIC_AUTH_USER`, `FITNICK_BASIC_AUTH_PASS` | Optional HTTP Basic credentials |
| `DJANGO_SECRET_KEY`, `DJANGO_ALLOWED_HOSTS`, `DJANGO_DEBUG` | Django configuration; use a private key and `DJANGO_DEBUG=0` in production |
| `FITNICK_DJANGO_DB_PATH` | SQLite location; defaults locally to `fitnick_django\db.sqlite3` |

OAuth access requires the relevant provider permissions. Sleep, resting heart
rate, HRV, and intraday heart-rate availability differ by provider and scopes.
Token refresh cannot grant missing scopes.

## Daily APIs

Each endpoint accepts required inclusive `from` and `to` dates in `YYYY-MM-DD`
format, with a maximum of **90 inclusive days**. Successful responses use a
`{"days": [...]}` envelope. Invalid dates/ranges return HTTP 400; provider errors
return a non-success response rather than fabricated success.

| Endpoint | Fields in each day |
|---|---|
| `/api/steps/daily` | `on_date`, `steps` (integer or `null` when unobserved) |
| `/api/goals/progress` | `on_date`, `steps`, `steps_goal`, `remaining_steps`, `percent`, `goal_reached` |
| `/api/sleep/daily` | `on_date`, `sleep_minutes`, `minutes_asleep`, `minutes_awake` |
| `/api/heart-rate/daily` | `on_date`, `resting_bpm`, `avg_bpm`, `min_bpm`, `max_bpm`, `hrv_ms` |

Google mode supplies daily resting BPM/HRV when available; intraday
`avg_bpm`/`min_bpm`/`max_bpm` remain null. Fitbit can supply those intraday
metrics when permissions allow. Heart/sleep rows without available metrics may
be omitted; individual fields may be null. `/api/steps/daily` returns every
requested date: an explicit provider count of zero remains `0`, while a date
without a provider observation is `null`. The dashboard-oriented
`/api/goals/progress` retains its existing default of zero when the summary is
missing. Daily responses do not repeat provider identity; `/health` reports the
selected provider, and each `on_date` is interpreted according to that
provider's civil-day contract.

Sleep rows use the provider's end-date attribution: Google civil end dates and
Fitbit `dateOfSleep`; sessions and naps attributed to the same date are
aggregated without shifting overnight sessions to the prior day. Google
`minutes_asleep` is actual asleep time. `sleep_minutes` is time in bed/session
duration, sourced from Fitbit `timeInBed`, Google summary asleep-plus-awake
minutes, or a valid interval duration when summaries are unavailable. A
partially available summary does not imply the missing metric is zero.
For multiple sessions on a date, an aggregate metric is null if any included
session lacks that metric; the response includes a warning for partial summaries.

For Google rows without a usable civil end date, a timezone-aware end timestamp
is converted to `FITNICK_TIME_ZONE` (default `America/New_York`) and a warning
is returned. Because provider-account timezone may differ, this fallback is
explicitly uncertain; a timestamp without timezone information is omitted with
a warning. Provider civil dates are not converted to the configured timezone.
Either daily endpoint may add a top-level `warnings` array with `code`,
`message`, and optional `on_date` when records are malformed, incomplete, out
of range, duplicated (steps), or date attribution needs attention. No warning
array means no mapping warning was detected; it does not guarantee provider
data completeness or freshness. Providers may revise historical totals after
late device syncs, and the current civil day may be incomplete.

```powershell
$env:FITNICK_API_KEY = "<your-configured-key>"
curl.exe --fail -H "X-API-Key: $env:FITNICK_API_KEY" "http://127.0.0.1:8002/api/steps/daily?from=2026-10-01&to=2026-10-07"
```

`/health` and `/healthz` report service/provider/mode and the deployed commit when
available; they do not verify provider connectivity. `/openapi.json` describes
the daily steps and sleep rows and their optional warnings.
`/health/smoke` and `/fitbit/smoke` can contact the configured provider and refresh
tokens; do not use them as offline tests.

## bCounter integration

`fitnick\integrations\bcounter.py` owns the existing wake-time sync. Configure
`BCOUNTER_BASE_URL`, `BCOUNTER_API_KEY`, and optionally `BCOUNTER_TIMEZONE`, then
explicitly run:

```powershell
.\.venv\Scripts\python.exe sync_bcounter_wake_time.py
```

This is a **live mutation** of bCounter state, not a test. The dashboard does not
automatically schedule this script. New cross-service activity/usage comparisons
belong in nsorchestrator; fitnick remains the owner of health data.

## Tests

Run from the repository root, using the repository-default import layout:

```powershell
.\.venv\Scripts\python.exe -m pytest tests -q
```

For focused API/dashboard work:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_daily_steps_api.py tests\test_daily_sleep_api.py tests\test_daily_heart_rate_api.py tests\test_dashboard_scope_handling.py tests\test_steps_goal_settings.py -q
```

Do not add the inner `fitnick_django` directory to `PYTHONPATH` for pytest:
tests import `fitnick_django.fitnick_django`. Django-marked tests can be skipped
if initialization fails; inspect skips instead of treating them as coverage.
Use mocked providers and isolated settings; never run real token refresh or
wake-time pushes as validation.

## Deployment and layout

Render installs the editable package plus Django/Gunicorn, runs Django migrations,
optionally bootstraps an admin, and starts
`fitnick_django.wsgi:application`. SQLite is on the persistent disk at
`/var/data/fitnick/db.sqlite3`; goal settings also need persistent storage.
The blueprint disables debug mode and enables authentication. Review
`render.yaml` for the exact command and environment variables.

- `fitnick_django\fitnick_django`: active views, templates, routes, auth, settings.
- `fitnick\base\live_api.py`: Google/Fitbit access and normalized metrics.
- `fitnick\integrations\bcounter.py`: existing wake-time bridge.
- `tests`: provider, Django API/dashboard, and integration regressions.
- `fitnick\activity`, `fitnick\apis`, `fitnick\flask_app`: older experiments.

The older packaging metadata is retained for compatibility; `setup.py` alone
does not install the entire deployed Django stack. `README.rst` is a short
packaging pointer to this guide, not a second setup document.
