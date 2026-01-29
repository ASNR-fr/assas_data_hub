"""Configuration file for the ASSAS Data Hub application."""

import os
from datetime import timedelta
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash

load_dotenv()


class Config(object):
    """Base configuration class for the ASSAS Data Hub application."""

    DEBUG = os.getenv("DEBUG", "True").lower() == "true"
    DEVELOPMENT = os.getenv("DEVELOPMENT", "True").lower() == "true"

    BASE_URL = os.getenv("BASE_URL", "/assas_app")
    AUTH_BASE_URL = os.getenv("AUTH_BASE_URL", "/auth")

    ASTEC_ROOT = os.getenv("ASTEC_ROOT", r"/root/astecV3.1.1_linux64/astecV3.1.1")
    ASTEC_TYPE = os.getenv("ASTEC_TYPE", r"linux_64")

    BACKUP_DIRECTORY = os.getenv(
        "BACKUP_DIRECTORY", r"Z:\\scc\\projects\\ASSAS\\backup_mongodb"
    )

    CONNECTIONSTRING = os.getenv("CONNECTIONSTRING", r"mongodb://localhost:27017/")
    CONNECTIONSTRING_ATLAS = os.getenv("CONNECTIONSTRING_ATLAS", "")

    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", r"assas")
    USER_COLLECTION_NAME = os.getenv("USER_COLLECTION_NAME", r"users")

    SERVER_NAME = os.getenv("SERVER_NAME", "assas.scc.kit.edu")
    PREFERRED_URL_SCHEME = os.getenv("PREFERRED_URL_SCHEME", "http")

    HELMHOLTZ_CLIENT_ID = os.getenv("HELMHOLTZ_CLIENT_ID", "")
    HELMHOLTZ_CLIENT_SECRET = os.getenv("HELMHOLTZ_CLIENT_SECRET", "")

    SECRET_KEY = os.getenv("SECRET_KEY", "dev-fallback-key-change-in-production")
    PERMANENT_SESSION_LIFETIME = timedelta(
        hours=int(os.getenv("SESSION_LIFETIME_HOURS", 8))
    )

    SESSION_PERMANENT = True
    SESSION_USE_SIGNER = True
    SESSION_COOKIE_SECURE = False
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_REFRESH_EACH_REQUEST = True
    SESSION_COOKIE_NAME = "assas_session"
    SESSION_COOKIE_DOMAIN = None
    SESSION_COOKIE_PATH = "/"

    ASSAS_GROUP_PREFIX = "urn:geant:helmholtz.de:group:ASSAS"

    AVAILABLE_ROLES = [
        {
            "value": "admin",
            "label": "Administrator",
            "description": "Full system access and user management",
        },
        {
            "value": "researcher",
            "label": "Researcher",
            "description": "Research data access and analysis tools",
        },
        {
            "value": "curator",
            "label": "Curator",
            "description": "Data curation and quality control",
        },
        {
            "value": "visitor",
            "label": "Visitor",
            "description": "Basic landing page access",
        },
    ]

    HELMHOLTZ_ENTITLEMENT_ROLE_MAP = {
        f"{ASSAS_GROUP_PREFIX}:admin": "admin",
        f"{ASSAS_GROUP_PREFIX}:curator": "curator",
        f"{ASSAS_GROUP_PREFIX}:researcher": "researcher",
    }


class DevelopmentConfig(Config):
    """Development configuration class for the ASSAS Data Hub application."""

    DEBUG = True
    PREFERRED_URL_SCHEME = "https"

    BASIC_AUTH_USERS = {
        "admin_local": {
            "password_hash": generate_password_hash("admin123"),
            "roles": ["admin"],
            "email": "admin@dev.local",
            "name": "Development Admin",
            "is_active": True,
        },
        "user": {
            "password_hash": generate_password_hash("user123"),
            "roles": ["visitor"],
            "email": "user@dev.local",
            "name": "Development User",
            "is_active": True,
        },
    }


class ProductionConfig(Config):
    """Production configuration class for the ASSAS Data Hub application."""

    DEBUG = False
    PREFERRED_URL_SCHEME = "https"
