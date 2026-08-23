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

Runs on top of Google Cloud Platform (https://console.cloud.google.com/) and uses `postgresql` as a database.  `PySpark` is used for data analysis & large querying - otherwise, `SQLAlchemy` is sufficient and is used instead.

Current direction
-------

The repo now includes a Render-first scaffold for ``fitnick_django`` so the web surface can act as a thin health integration service instead of assuming full PostgreSQL replication of API data.

Two endpoints are intended for early deployment validation:

* ``/healthz`` confirms the Django service is up.
* ``/health/smoke`` attempts a live API identity request through the currently selected provider and reports whether credentials work.
* ``/fitbit/smoke`` remains as a compatibility alias to ``/health/smoke``.

The data-model code under ``fitnick`` still supports historical sync work, but the deployment scaffold now favors live API reads for current-state views and a Google Health migration path.

Google Health token quickstart
-------

1. Create a Google OAuth client and enable the Google Health API (see https://developers.google.com/health/setup).
2. Generate a consent URL:

   ``python google_health_token.py auth-url --client-id "<client-id>" --prompt-consent``

3. Open the URL, approve scopes, and copy the returned ``code`` query parameter.
4. Exchange the code for tokens:

   ``python google_health_token.py exchange-code --client-id "<client-id>" --client-secret "<client-secret>" --code "<code>" --print-env``

5. Apply the printed environment variables, then verify:

   ``python fitnick_django/manage.py runserver`` and open ``/health/smoke``.

Local persistent `.env` setup
-------

To persist credentials between terminal sessions:

1. Copy ``.env.example`` to ``.env`` at the repository root.
2. Fill in your real ``GOOGLE_HEALTH_*`` values.
3. Restart ``runserver``.

``fitnick_django`` automatically loads ``.env`` on startup for local development.

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
