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

Tests passing:

.. image:: https://i.imgur.com/LuRgElm.png
        :target: https://i.imgur.com/LuRgElm.png

Runs on top of Google Cloud Platform (https://console.cloud.google.com/) and uses `postgresql` as a database.  `PySpark` is used for data analysis & large querying - otherwise, `SQLAlchemy` is sufficient and is used instead.

* Free software: MIT license
* Documentation: https://fitnick.readthedocs.io.


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
