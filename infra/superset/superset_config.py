"""
Superset configuration for CCE Data Pipeline.
Includes ClickHouse connection, Keycloak OAuth2, and caching.
"""
import os
from datetime import timedelta

# -----------------------------------------------------------------
# General
# -----------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE-ME-IN-PRODUCTION")
APP_NAME = "CCE Analytics"

# -----------------------------------------------------------------
# Database (Superset metadata)
# -----------------------------------------------------------------
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URI",
    "postgresql+psycopg2://superset:superset_dev@superset-db:5432/superset"
)

# -----------------------------------------------------------------
# Cache (Redis)
# -----------------------------------------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": REDIS_URL,
}
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": REDIS_URL,
}
FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_URL": REDIS_URL,
}

# -----------------------------------------------------------------
# Celery (async queries & scheduled reports)
# -----------------------------------------------------------------
class CeleryConfig:
    broker_url = REDIS_URL
    result_backend = REDIS_URL
    imports = ("superset.sql_lab", "superset.tasks.scheduler")
    task_annotations = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
    }
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": timedelta(minutes=1),
        },
    }

CELERY_CONFIG = CeleryConfig

# -----------------------------------------------------------------
# OAuth2 / Keycloak
# -----------------------------------------------------------------
AUTH_TYPE = 2  # AUTH_OAUTH
OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ.get("KEYCLOAK_CLIENT_ID", "superset"),
            "client_secret": os.environ.get("KEYCLOAK_CLIENT_SECRET", ""),
            "api_base_url": os.environ.get("KEYCLOAK_BASE_URL", "http://keycloak:8080/realms/cce/protocol/openid-connect"),
            "access_token_url": os.environ.get("KEYCLOAK_TOKEN_URL", "http://keycloak:8080/realms/cce/protocol/openid-connect/token"),
            "authorize_url": os.environ.get("KEYCLOAK_AUTHORIZE_URL", "http://keycloak:8080/realms/cce/protocol/openid-connect/auth"),
            "server_metadata_url": os.environ.get("KEYCLOAK_METADATA_URL", "http://keycloak:8080/realms/cce/.well-known/openid-configuration"),
            "client_kwargs": {"scope": "openid email profile"},
        },
    }
]

# Map Keycloak roles to Superset roles
AUTH_ROLE_ADMIN = "Admin"
AUTH_ROLE_PUBLIC = "Public"
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Gamma"

# -----------------------------------------------------------------
# Feature flags
# -----------------------------------------------------------------
FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": True,
    "SCHEDULED_QUERIES": True,
}

# -----------------------------------------------------------------
# ClickHouse connection (default analytics database)
# -----------------------------------------------------------------
SQLALCHEMY_CUSTOM_PASSWORD_STORE = None

# Row-level security
ENABLE_ROW_LEVEL_SECURITY = True

# Alerts & Reports
ALERT_REPORTS_NOTIFICATION_DRY_RUN = os.environ.get("ALERT_DRY_RUN", "false").lower() == "true"
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.example.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_STARTTLS = True
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_MAIL_FROM = os.environ.get("SMTP_MAIL_FROM", "cce-analytics@example.com")
