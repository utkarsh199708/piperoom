"""Airflow webserver config for the dev `batch` profile (dev/batch.md).

AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.default only skips
authentication; since Airflow 2.8 every API call is still authorized by the
FAB auth manager, and an anonymous user without a public role gets 403. The
public role below gives anonymous callers Viewer rights: read-only, which is
all internal/clients/airflow needs. The port is bound to 127.0.0.1 only.
"""

from flask_appbuilder.const import AUTH_DB

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None
AUTH_TYPE = AUTH_DB
AUTH_ROLE_PUBLIC = "Viewer"
