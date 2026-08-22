=======
fitnick
=======

.. image:: https://readthedocs.org/projects/fitnick/badge/?version=latest
        :target: https://fitnick.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://img.shields.io/travis/kcinnick/fitnick.svg
        :target: https://travis-ci.com/kcinnick/fitnick

Hacking around on the Python implementation of the Fitbit API with my own Fitbit.

I created this for my own curiosity, but if you'd like to use it, you'll have to set the environment variables for ``FITBIT_CONSUMER_KEY``, ``FITBIT_CONSUMER_SECRET``, ``FITBIT_ACCESS_KEY`` and ``FITBIT_REFRESH_TOKEN`` using this tutorial: https://dev.fitbit.com/apps/oauthinteractivetutorial?clientEncodedId=&clientSecret=&redirectUri=https://dev.fitbit.com/&applicationType=SERVER.

Runs on top of Google Cloud Platform (https://console.cloud.google.com/) and uses `postgresql` as a database.  `PySpark` is used for data analysis & large querying - otherwise, `SQLAlchemy` is sufficient and is used instead.

Current direction
-------

The repo now includes a Render-first scaffold for ``fitnick_django`` so the web surface can act as a thin Fitbit integration service instead of assuming full PostgreSQL replication of Fitbit data.

Two endpoints are intended for early deployment validation:

* ``/healthz`` confirms the Django service is up.
* ``/fitbit/smoke`` attempts a live Fitbit profile request with ``FITBIT_ACCESS_TOKEN`` or ``FITBIT_ACCESS_KEY`` and reports whether the current credentials work.

The data-model code under ``fitnick`` still supports historical sync work, but the deployment scaffold now favors live API reads for current-state views.

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
