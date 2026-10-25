=====
Usage
=====

To use fitnick in a project::

    import fitnick

Sibling-service integrations
===========================

Priorities and ownership
------------------------

1. **nsorchestrator: daily activity joins.** Consume Fitnick's daily steps and
   goal-progress APIs alongside the existing daily heart-rate API. Join on
   ``on_date`` to compare steps with stress-tagged journal days. Fitnick provides
   read-only metrics; aggregation, scheduled ingestion and the unified dashboard
   belong in nsorchestrator.
2. **bCounter: build on the existing wake-time sync.** Run
   ``python sync_bcounter_wake_time.py`` with ``BCOUNTER_BASE_URL`` and
   ``BCOUNTER_API_KEY`` configured. It sends the latest sleep end time to
   ``/wake-time`` only for the current local day and skips an unchanged timestamp.
   Use ``BCOUNTER_TIMEZONE`` (default ``America/New_York``) consistently with
   Fitnick. Hit-count/duration joins with HR, HRV and sleep are a next phase:
   bCounter's daily export contract must be confirmed before implementing them.

The contracts below are implemented in Fitnick. No scheduler, sibling-service
changes, historical sleep export, or cross-service dashboard is installed here.
Sibling repositories were not accessible during this implementation; their
server-side auth and ingestion contracts still need confirmation.

Daily API contract
------------------

All daily routes accept required ``from`` and ``to`` query parameters in
``YYYY-MM-DD`` form. Bounds are inclusive, ordered, and limited to 90 days.
Dates are provider civil days, not UTC timestamps; align consumers with
``FITNICK_TIME_ZONE`` (default ``America/New_York``) and the provider's timezone.
Use HTTPS and keep keys in environment variables or a secret manager::

    curl --get "$FITNICK_BASE_URL/api/steps/daily" \
      --header "X-API-Key: $FITNICK_API_KEY" \
      --data-urlencode "from=2026-10-01" \
      --data-urlencode "to=2026-10-01"

``GET /api/steps/daily`` returns ascending daily rows::

    {"days": [{"on_date": "2026-10-01", "steps": 10500}]}

``GET /api/goals/progress`` accepts the same window and returns::

    {"days": [{
      "on_date": "2026-10-01",
      "steps": 10500,
      "steps_goal": 10000,
      "remaining_steps": 0,
      "percent": 105.0,
      "goal_reached": true
    }]}

Both endpoints support Google Health and Fitbit using the dashboard's activity
summary mapping, with one provider read per day. Missing step fields or empty
Google rollups normalize to zero, as on the dashboard; zero is therefore not
proof of complete tracking. Today's values are provisional. Provider failures
return an error for the entire window, not a successful partial result.

Goal precedence matches the dashboard: current saved override, provider-returned
goal, then ``FITNICK_DEFAULT_STEPS_GOAL`` (default 10000). Google does not expose a
goal. Overrides/defaults are evaluated at request time, **not historical goal
snapshots**. ``remaining_steps`` floors at zero; ``percent`` is rounded to two
decimal places and may exceed 100. ``goal_reached`` includes exact equality.

``GET /api/heart-rate/daily`` uses the same window and returns ``on_date``,
``resting_bpm``, ``avg_bpm``, ``min_bpm``, ``max_bpm`` and ``hrv_ms`` in ``days``.
Unavailable heart fields are ``null``; days with no heart metrics are omitted.
Google currently supplies resting HR/HRV only.

Invalid windows return 400, unauthorized requests return 401 when auth is enabled,
and the new activity endpoints reject non-GET methods with 405. Provider errors
retain their upstream HTTP status; missing configuration returns 503.
Error bodies have ``{"ok": false, "error": "..."}`` (405 uses Django's standard
method-not-allowed response). Activity provider failures use a generic message
without exposing upstream exception details. Machine-readable activity schemas
and auth metadata are available at ``/openapi.json``.

Authentication conventions
--------------------------

For a deployed integration, set ``FITNICK_REQUIRE_AUTH=1`` and a nonempty
``FITNICK_API_KEY`` on Fitnick. Send that key in ``X-API-Key`` on every request;
never put it in query strings. Authentication is opt-in in existing Fitnick
deployments; setting the key alone does not enable enforcement. Daily endpoints
are not exempt by default; do not add them to ``FITNICK_AUTH_EXEMPT_PATHS``.
Existing session and optional Basic auth remain supported.

Fitnick's outbound bCounter client also sends ``X-API-Key``, using the separate
``BCOUNTER_API_KEY``. This confirms the header convention in Fitnick's clients
and middleware, not in inaccessible sibling servers. Before rollout, verify
each sibling rejects missing/invalid keys, accepts the configured key, and
supports key rotation. Do not reuse provider OAuth tokens as service API keys.

Proposed nsorchestrator ingestion contract
-----------------------------------------

Store one summary per service user and civil date, including a schema version
and timezone. A proposed version-1 normalized row is::

    {
      "schema_version": 1,
      "source": "fitnick",
      "on_date": "2026-10-01",
      "time_zone": "America/New_York",
      "steps": 10500,
      "steps_goal": 10000,
      "goal_reached": true,
      "resting_bpm": 62,
      "hrv_ms": 42.5,
      "sleep_minutes": null
    }

This is a consumer-side proposal, not another Fitnick endpoint. Preserve missing
heart/sleep values as ``null`` rather than zero. Daily sleep needs its own export
contract; the existing latest-sleep read is not a historical daily series.

Schedule ingestion in nsorchestrator after local midnight, re-reading the last
seven civil days to capture late provider updates. Fetch goal-progress once
(it already includes steps), then heart-rate for the same window. Upsert by
service user, source and ``on_date``; do not append duplicate daily records.
Use windows of at most 90 days for backfills, explicit network timeouts, bounded
backoff for 429/5xx, and no retries for bad credentials or invalid windows.
Do not overwrite stored metrics with an error response. Surface freshness and
provider gaps on the unified dashboard.

Follow-on sibling joins
----------------------

* **nsmemory:** join mood/tags by civil date. Propose goal-reached memories with
  a stable deduplication key (user, source, date, milestone type). Evaluate
  completed days only; record the goal used because settings can change.
  Streaks require consecutive tracked days, and unusual-HRV events require an
  agreed baseline/minimum sample size. Confirm its write API before auto-logging.
* **nsibadah:** join prayer, fasting and Ramadan dates with activity/HR, and sleep
  once exported. Prayer-time step reminders need a confirmed timezone, reminder
  API and user opt-in; they are not scheduled by Fitnick.
* **nstv:** join daily viewing minutes with steps and, once available, sleep.
  Agree how sessions crossing midnight are split before comparing daily totals.

These joins describe associations, not causal or medical conclusions. Confirm
each service's date boundaries, units, missing-data semantics and API-key auth
before enabling the next integration.
