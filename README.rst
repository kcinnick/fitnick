=======
fitnick
=======

.. image:: https://readthedocs.org/projects/fitnick/badge/?version=latest
        :target: https://fitnick.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://img.shields.io/travis/kcinnick/fitnick.svg
        :target: https://travis-ci.com/kcinnick/fitnick

Hacking around on health data integrations with a migration path from Fitbit Web APIs to Google Health APIs.

I created this for my own curiosity, but if you'd like to use it for live API reads, configure ``FITNICK_HEALTH_PROVIDER`` with ``google`` (default) or ``fitbit`` and provide the matching access token:

* Google Health: ``GOOGLE_HEALTH_ACCESS_TOKEN``
* Fitbit (legacy): ``FITBIT_ACCESS_TOKEN`` or ``FITBIT_ACCESS_KEY``

Historically this project used Google Cloud Platform and PostgreSQL for bulk sync analysis. The current Render-first service defaults to Django + SQLite on a Render Disk for auth/session persistence, while PostgreSQL remains optional for historical analysis workloads.

Current direction
-------

The repo now includes a Render-first scaffold for ``fitnick_django`` so the web surface can act as a thin health integration service instead of assuming full PostgreSQL replication of API data.

Two endpoints are intended for early deployment validation:

* ``/healthz`` confirms the Django service is up.
* ``/health/smoke`` attempts a live API identity request through the currently selected provider and reports whether credentials work.
* ``/fitbit/smoke`` remains as a compatibility alias to ``/health/smoke``.

The data-model code under ``fitnick`` still supports historical sync work, but the deployment scaffold now favors live API reads for current-state views and a Google Health migration path.

Render deployment checklist
-------

1. Push this repo to GitHub and create a new Blueprint service in Render using ``render.yaml``.
2. In Render service environment variables, set:

   * ``FITNICK_HEALTH_PROVIDER=google``
   * ``FITNICK_OFFLINE_MODE=0``
   * ``FITNICK_AUTO_REFRESH_TOKENS=1``
   * ``FITNICK_REQUIRE_AUTH=1``
   * ``FITNICK_AUTH_EXEMPT_PATHS=/healthz,/login,/logout,/admin/login``
   * ``FITNICK_API_KEY=<long-random-key>`` and/or ``FITNICK_BASIC_AUTH_USER`` + ``FITNICK_BASIC_AUTH_PASS``
   * ``GOOGLE_HEALTH_CLIENT_ID=<client-id>``
   * ``GOOGLE_HEALTH_CLIENT_SECRET=<client-secret>``
   * ``GOOGLE_HEALTH_REFRESH_TOKEN=<refresh-token>``
   * ``GOOGLE_HEALTH_ACCESS_TOKEN=<access-token>`` (optional but recommended for first boot)

3. Deploy and wait for startup migration + gunicorn boot.

   Persistent auth storage on Render:

   * Attach a Render Disk (the Blueprint now defines ``/var/data``).
   * Keep ``FITNICK_DJANGO_DB_PATH=/var/data/fitnick/db.sqlite3`` so SQLite survives restarts/redeploys.
   * If this value points at ephemeral filesystem, users/sessions can disappear after deploys.

4. Validate health and auth:

   * ``https://<your-render-host>/healthz`` should return ``{"status": "ok", ...}``
   * ``https://<your-render-host>/health/smoke`` should return ``{"ok": true, ...}``
   * ``https://<your-render-host>/login`` should show the sign-in page

5. Create an app user for session login (run in Render Shell):

   ``python fitnick_django/manage.py createsuperuser``

   Optional bootstrap flow (env-driven):

   * ``FITNICK_BOOTSTRAP_ADMIN_ENABLED=1``
   * ``FITNICK_BOOTSTRAP_ADMIN_USERNAME=<admin-user>``
   * ``FITNICK_BOOTSTRAP_ADMIN_PASSWORD=<strong-password>``
   * ``FITNICK_BOOTSTRAP_ADMIN_EMAIL=<email>`` (optional)
   * ``FITNICK_BOOTSTRAP_ADMIN_RESET_PASSWORD=0`` (set ``1`` only when intentionally rotating via deploy)

   With these set, startup will auto-create the admin user after migrations.
   After first successful login, set ``FITNICK_BOOTSTRAP_ADMIN_ENABLED=0`` when persistent DB storage is configured.

   Manual bootstrap trigger (Render Shell):

   ``python fitnick_django/manage.py shell -c "from fitnick_django.bootstrap_admin import bootstrap_admin_user; bootstrap_admin_user()"``


Notes:

* Access tokens rotate. ``fitnick`` now attempts automatic refresh when a live request receives a 401 and refresh credentials are present.
* Keep ``GOOGLE_HEALTH_REFRESH_TOKEN`` current in Render env vars; if Google rotates it, update it in Render.
* Protected endpoints require one of: Django login session, ``X-API-Key``, or HTTP Basic credentials when auth is enabled.
* If neither ``FITNICK_API_KEY`` nor HTTP Basic creds are configured, unauthenticated requests to protected routes (such as ``/``) return ``503`` until you authenticate via ``/login``.

Protected endpoint examples
-------

API key:

``curl -H "X-API-Key: <your-key>" https://<your-render-host>/health/smoke``

HTTP Basic auth:

``curl -u "<user>:<pass>" https://<your-render-host>/``

bCounter wake-time sync
-------

FitNick can send the end of today's latest Google Health sleep session to
bCounter's authenticated ``POST /wake-time`` endpoint. Configure:

* ``BCOUNTER_BASE_URL=https://<your-bcounter-host>``
* ``BCOUNTER_API_KEY=<the same value as bCounter's BCOUNTER_API_KEY>``
* ``BCOUNTER_TIMEZONE=America/New_York`` (or the timezone bCounter uses)
* ``FITNICK_OVERNIGHT_SLEEP_MINUTES=180`` (minimum duration used to exclude naps)

Run a sync manually:

``python sync_bcounter_wake_time.py``

FitNick selects the longest overnight session on the most recent overnight
wake date, rather than a later short nap. The command only pushes a wake time
for the current date in ``BCOUNTER_TIMEZONE``. It checks bCounter first and
skips an exact duplicate; if bCounter has a different time, it sends the
health-derived value and bCounter keeps the earliest entry for that day.

For automation, create a Render Cron Job from this repository with the same
``GOOGLE_HEALTH_*`` and ``BCOUNTER_*`` secrets as the web service. Use
``python sync_bcounter_wake_time.py`` as the command. An hourly schedule is
safe because duplicate values are skipped; restrict the schedule to likely
waking hours if desired.

Google Health token quickstart
-------

1. Create a Google OAuth client and enable the Google Health API (see https://developers.google.com/health/setup).
2. Generate a consent URL:

   ``python google_health_token.py auth-url --client-id "<client-id>" --prompt-consent``

3. Open the URL, approve scopes, and copy the returned ``code`` query parameter.
4. Exchange the code for tokens:

   ``python google_health_token.py exchange-code --client-id "<client-id>" --client-secret "<client-secret>" --code "<code>" --print-env``

   To persist directly into your local ``.env``:

   ``python google_health_token.py exchange-code --client-id "<client-id>" --client-secret "<client-secret>" --code "<code>" --write-env-file .env``

5. Apply the printed environment variables, then verify:

   ``python fitnick_django/manage.py runserver`` and open ``/health/smoke``.

Programmatic refresh from `.env`
-------

Once ``.env`` contains ``GOOGLE_HEALTH_CLIENT_ID``, ``GOOGLE_HEALTH_CLIENT_SECRET``, and ``GOOGLE_HEALTH_REFRESH_TOKEN``, refresh with:

``python google_health_token.py refresh-from-env --env-file .env``

This updates ``GOOGLE_HEALTH_ACCESS_TOKEN`` (and ``HEALTH_ACCESS_TOKEN``) in-place.

Local persistent `.env` setup
-------

To persist credentials between terminal sessions:

1. Copy ``.env.example`` to ``.env`` at the repository root.
2. Fill in your real ``GOOGLE_HEALTH_*`` values.
3. Restart ``runserver``.

``fitnick_django`` automatically loads ``.env`` on startup for local development.

Local dashboard
-------

With tokens configured, run ``python fitnick_django/manage.py runserver`` and open ``http://127.0.0.1:8000/`` to view the local Fitbit-style dashboard shell:

* Steps today progress
* 7-day steps trend
* Latest sleep snapshot
* Latest body-fat snapshot

* Free software: MIT license
* Documentation: https://fitnick.readthedocs.io.


Installation
-------

``pip install fitnick``


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
