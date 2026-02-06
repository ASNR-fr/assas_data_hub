"""Admin page for the ASSAS Data Hub application.

This module provides administrative functionality for managing users,
monitoring system status, and accessing administrative tools.
Only accessible to users with admin role.
"""

import logging
import dash
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import io
import base64
import json
import re

from dash import html, dcc, callback, Input, Output, State, dash_table, ctx
from dash.exceptions import PreventUpdate
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple, Union
from werkzeug.security import generate_password_hash

from ...auth_utils import get_current_user, require_role
from ...database.user_manager import UserManager

logger = logging.getLogger("assas_app")

_EDITABLE_COLS = {
    "username",
    "email",
    "name",
    "roles",
    "is_active",
    "institute",
    "batch",
}
_PROTECTED_COLS = {"_id", "id", "provider", "created_at", "last_login", "login_count"}

_DEFAULT_VISIBLE_COLS = {
    "username",
    "email",
    "name",
    "provider",
    "roles",
    "is_active",
    "institute",
    "batch",
    "last_login",
    "login_count",
    "created_at",
}

_SENSITIVE_KEY_RE = re.compile(r"password|secret|token|api[_-]?key|hash", re.IGNORECASE)


def _safe_cell_value(v: object) -> object:
    """Convert complex types to string for safe display in table cells."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (dict, list)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return str(v)


def _parse_roles(v: object) -> List[str]:
    """Accept 'admin, visitor' or ['admin','visitor'] -> list[str]."""
    if v is None:
        return ["visitor"]
    if isinstance(v, list):
        out = [str(x).strip() for x in v if str(x).strip()]
        return out or ["visitor"]
    if isinstance(v, str):
        parts = [p.strip() for p in v.split(",")]
        out = [p for p in parts if p]
        return out or ["visitor"]
    return ["visitor"]


def _parse_is_active(v: object) -> bool:
    """Accept True/False, ✓/✗, 'true'/'false'."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"✓", "true", "1", "yes", "y", "on"}:
            return True
        if s in {"✗", "false", "0", "no", "n", "off"}:
            return False
    # default to True to avoid accidental lockouts from bad input
    return True


def _sanitize_patch(curr: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, Any]:
    """Build a safe update dict from an edited row."""
    patch: Dict[str, Any] = {}

    for k in _EDITABLE_COLS:
        if k not in curr:
            continue
        if curr.get(k) == prev.get(k):
            continue

        if k == "email":
            patch["email"] = (curr.get("email") or "").strip().lower()
        elif k == "username":
            patch["username"] = (curr.get("username") or "").strip()
        elif k == "roles":
            patch["roles"] = _parse_roles(curr.get("roles"))
        elif k == "is_active":
            patch["is_active"] = _parse_is_active(curr.get("is_active"))
        elif k == "institute":
            patch["institute"] = (curr.get("institute") or "").strip()
        elif k == "batch":
            patch["batch"] = (curr.get("batch") or "").strip()
        else:
            patch[k] = curr.get(k)

    # defense-in-depth
    for k in list(patch.keys()):
        if k in _PROTECTED_COLS:
            patch.pop(k, None)

    return patch


def _validate_custom_field_name(name: str) -> Optional[str]:
    """Validate a custom field name for user properties."""
    if not name or not isinstance(name, str):
        return "Property name is required."
    n = name.strip()

    # Prevent MongoDB operator / path injection
    if n.startswith("$") or "." in n:
        return "Property name cannot start with '$' or contain '.'."

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", n):
        return "Property name must match: [A-Za-z_][A-Za-z0-9_]*"

    if n in _PROTECTED_COLS:
        return f"Property name '{n}' is not allowed."

    return None


def _parse_custom_value(raw: object, typ: str) -> object:
    if typ == "string":
        return "" if raw is None else str(raw)
    if typ == "number":
        if raw is None or str(raw).strip() == "":
            return None
        s = str(raw).strip()
        return float(s) if "." in s else int(s)
    if typ == "boolean":
        return _parse_is_active(raw)
    if typ == "json":
        if raw is None or str(raw).strip() == "":
            return None
        return json.loads(raw)
    return raw


# Admin styling
ADMIN_CARD_STYLE = {
    "margin": "10px 0",
    "box-shadow": "0 4px 6px rgba(0, 0, 0, 0.1)",
    "border": "1px solid #e0e0e0",
}

STAT_CARD_STYLE = {
    "text-align": "center",
    "padding": "20px",
    "margin": "10px",
    "background": "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
    "color": "white",
    "border-radius": "10px",
    "box-shadow": "0 4px 15px rgba(0, 0, 0, 0.2)",
}


def get_user_stats() -> Dict[str, Any]:
    """Get comprehensive user statistics from MongoDB."""
    try:
        user_manager = UserManager()
        all_users = user_manager.get_all_users()

        now = datetime.now(timezone.utc)

        stats = {
            "total_users": len(all_users),
            "active_users": 0,
            "inactive_users": 0,
            "helmholtz_users": 0,
            "basic_auth_users": 0,
            "oauth_with_basic_users": 0,
            "admin_users": 0,
            "researcher_users": 0,
            "curator_users": 0,
            "visitor_users": 0,
            "recent_logins_24h": 0,
            "recent_logins_7d": 0,
            "recent_logins_30d": 0,
            "never_logged_in": 0,
            "users_by_institute": {},
            "users_by_provider": {},
            "users_with_avatars": 0,
        }

        for user in all_users:
            # Activity status
            if user.get("is_active", True):
                stats["active_users"] += 1
            else:
                stats["inactive_users"] += 1

            # Provider statistics
            provider = user.get("provider", "unknown")
            stats["users_by_provider"][provider] = (
                stats["users_by_provider"].get(provider, 0) + 1
            )

            if provider == "helmholtz":
                stats["helmholtz_users"] += 1
            elif provider == "basic_auth":
                stats["basic_auth_users"] += 1

            # Check for OAuth users with basic auth
            if provider in ["helmholtz"] and (
                user.get("basic_auth_password_hash")
                or user.get("temp_basic_auth_password_hash")
            ):
                stats["oauth_with_basic_users"] += 1

            # Role statistics
            roles = user.get("roles", [])
            if isinstance(roles, list):
                if "admin" in roles:
                    stats["admin_users"] += 1
                if "researcher" in roles:
                    stats["researcher_users"] += 1
                if "curator" in roles:
                    stats["curator_users"] += 1
                if "visitor" in roles:
                    stats["visitor_users"] += 1
            elif isinstance(roles, str):
                if roles == "admin":
                    stats["admin_users"] += 1
                elif roles == "researcher":
                    stats["researcher_users"] += 1
                elif roles == "curator":
                    stats["curator_users"] += 1
                elif roles == "visitor":
                    stats["visitor_users"] += 1

            # Institute statistics
            institute = user.get("institute", "Unknown")
            stats["users_by_institute"][institute] = (
                stats["users_by_institute"].get(institute, 0) + 1
            )

            # Avatar statistics
            if user.get("avatar_url"):
                stats["users_with_avatars"] += 1

            # Login statistics
            last_login = user.get("last_login")
            if last_login:
                try:
                    if isinstance(last_login, str):
                        if "T" in last_login:
                            if last_login.endswith("Z"):
                                last_login_dt = datetime.fromisoformat(
                                    last_login.replace("Z", "+00:00")
                                )
                            else:
                                last_login_dt = datetime.fromisoformat(last_login)
                        else:
                            last_login_dt = datetime.strptime(last_login, "%Y-%m-%d")
                    elif hasattr(last_login, "replace"):
                        last_login_dt = (
                            last_login.replace(tzinfo=None)
                            if last_login.tzinfo
                            else last_login
                        )
                    else:
                        continue

                    time_diff = now - last_login_dt

                    if time_diff.days == 0:
                        stats["recent_logins_24h"] += 1
                    if time_diff.days <= 7:
                        stats["recent_logins_7d"] += 1
                    if time_diff.days <= 30:
                        stats["recent_logins_30d"] += 1

                except (ValueError, TypeError, AttributeError) as e:
                    logger.warning(
                        f"Could not parse last_login for "
                        f"user {user.get('username', 'unknown')}: {e}"
                    )
                    continue
            else:
                stats["never_logged_in"] += 1

        logger.info(f"Calculated comprehensive user statistics: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Error getting user stats: {e}")
        return {
            "total_users": 0,
            "active_users": 0,
            "inactive_users": 0,
            "helmholtz_users": 0,
            "basic_auth_users": 0,
            "oauth_with_basic_users": 0,
            "admin_users": 0,
            "researcher_users": 0,
            "curator_users": 0,
            "visitor_users": 0,
            "recent_logins_24h": 0,
            "recent_logins_7d": 0,
            "recent_logins_30d": 0,
            "never_logged_in": 0,
            "users_by_institute": {},
            "users_by_provider": {},
            "users_with_avatars": 0,
        }


def get_users_data() -> List[Dict]:
    """Get comprehensive users data for the table."""
    try:
        user_manager = UserManager()
        all_users = user_manager.get_all_users()

        # Include custom properties as additional columns (hideable in UI).
        base_cols = {
            "id",
            "_id",
            "username",
            "email",
            "name",
            "provider",
            "roles",
            "is_active",
            "institute",
            "batch",
            "last_login",
            "created_at",
            "login_count",
        }

        extra_keys: set[str] = set()
        for u in all_users:
            for k in (u or {}).keys():
                if k in base_cols or k in _PROTECTED_COLS:
                    continue
                if k.startswith("_"):
                    continue
                if _SENSITIVE_KEY_RE.search(k):
                    continue
                extra_keys.add(k)

        users_data = []
        for user in all_users:
            try:
                # Format last login safely
                last_login = user.get("last_login")
                if last_login:
                    try:
                        if isinstance(last_login, str):
                            if "T" in last_login:
                                if last_login.endswith("Z"):
                                    last_login_dt = datetime.fromisoformat(
                                        last_login.replace("Z", "+00:00")
                                    )
                                else:
                                    last_login_dt = datetime.fromisoformat(last_login)
                                last_login_str = last_login_dt.strftime(
                                    "%Y-%m-%d %H:%M"
                                )
                            else:
                                last_login_str = last_login
                        elif hasattr(last_login, "strftime"):
                            last_login_str = last_login.strftime("%Y-%m-%d %H:%M")
                        else:
                            last_login_str = str(last_login)
                    except Exception as e:
                        logger.error(
                            f"Error formatting last login for user "
                            f"{user.get('username', 'unknown')}: {e}"
                        )
                        last_login_str = "Invalid date"
                else:
                    last_login_str = "Never"

                # Format creation date safely
                created_at = user.get("created_at")
                if created_at:
                    try:
                        if isinstance(created_at, str):
                            if "T" in created_at:
                                if created_at.endswith("Z"):
                                    created_dt = datetime.fromisoformat(
                                        created_at.replace("Z", "+00:00")
                                    )
                                else:
                                    created_dt = datetime.fromisoformat(created_at)
                                created_str = created_dt.strftime("%Y-%m-%d")
                            else:
                                created_str = created_at
                        elif hasattr(created_at, "strftime"):
                            created_str = created_at.strftime("%Y-%m-%d")
                        else:
                            created_str = str(created_at)
                    except Exception as e:
                        logger.error(
                            f"Error formatting created at for user "
                            f"{user.get('username', 'unknown')}: {e}"
                        )
                        created_str = "Invalid date"
                else:
                    created_str = "Unknown"

                # Handle roles safely
                roles = user.get("roles", [])
                if isinstance(roles, list):
                    roles_str = ", ".join(roles)
                elif isinstance(roles, str):
                    roles_str = roles
                else:
                    roles_str = str(roles) if roles else "No roles"

                # Determine authentication methods
                provider = user.get("provider", "")
                has_basic_auth = bool(
                    user.get("basic_auth_password_hash")
                    or user.get("temp_basic_auth_password_hash")
                    or user.get("password")  # Legacy field
                )

                auth_methods = []
                if provider == "basic_auth" or has_basic_auth:
                    auth_methods.append("Basic")
                if provider in ["helmholtz"]:
                    auth_methods.append(provider.upper())

                auth_methods_str = (
                    ", ".join(auth_methods)
                    if auth_methods
                    else provider.upper()
                    if provider
                    else "Unknown"
                )

                user_id = str(user.get("_id", ""))

                row: Dict[str, Any] = {
                    # Keep existing "id" AND provide "_id" (hidden) for updates
                    "id": user_id,
                    "_id": user_id,
                    "username": user.get("username", ""),
                    "email": user.get("email", ""),
                    "name": user.get("name", ""),
                    "provider": auth_methods_str,
                    "roles": roles_str,
                    # Keep ✓/✗ so existing styling keeps working,
                    # but it is editable via dropdown now
                    "is_active": "✓" if user.get("is_active", True) else "✗",
                    "institute": user.get("institute", ""),
                    "batch": user.get("batch", ""),
                    "last_login": last_login_str,
                    "created_at": created_str,
                    "login_count": user.get("login_count", 0),
                }

                for k in extra_keys:
                    row[k] = _safe_cell_value(user.get(k))

                users_data.append(row)

            except Exception as e:
                logger.error(
                    f"Error processing user {user.get('username', 'unknown')}: {e}."
                )

        logger.info(f"Processed {len(users_data)} users for admin table")
        return users_data

    except Exception as e:
        logger.error(f"Error getting users data: {e}")
        return []


def create_charts(stats: Dict[str, Any]) -> html.Div:
    """Create enhanced charts for user analytics."""
    charts = []

    try:
        # Provider distribution chart
        provider_data = stats.get("users_by_provider", {})
        if provider_data:
            provider_fig = px.pie(
                values=list(provider_data.values()),
                names=list(provider_data.keys()),
                title="Users by Authentication Provider",
            )
            provider_fig.update_layout(height=300)
            charts.append(dbc.Col([dcc.Graph(figure=provider_fig)], md=6))
    except Exception as e:
        logger.error(f"Error creating provider chart: {e}")

    try:
        # Role distribution chart
        role_data = {
            "Admin": stats.get("admin_users", 0),
            "Researcher": stats.get("researcher_users", 0),
            "Curator": stats.get("curator_users", 0),
            "Visitor": stats.get("visitor_users", 0),
        }

        # Only create chart if we have data
        if any(role_data.values()):
            role_fig = px.bar(
                x=list(role_data.keys()),
                y=list(role_data.values()),
                title="Users by Role",
            )
            role_fig.update_layout(height=300)
            charts.append(dbc.Col([dcc.Graph(figure=role_fig)], md=6))
    except Exception as e:
        logger.error(f"Error creating role chart: {e}")

    try:
        # Institute distribution chart (fixed)
        institute_data = stats.get("users_by_institute", {})
        if institute_data and len(institute_data) > 0:
            # Limit to top 10 institutes for readability
            sorted_institutes = sorted(
                institute_data.items(), key=lambda x: x[1], reverse=True
            )[:10]

            if sorted_institutes:
                # Create DataFrame for proper chart creation
                df_institutes = pd.DataFrame(
                    sorted_institutes, columns=["Institute", "Count"]
                )

                institute_fig = px.bar(
                    df_institutes,
                    x="Count",
                    y="Institute",
                    orientation="h",
                    title="Users by Institute (Top 10)",
                )
                institute_fig.update_layout(
                    height=400, yaxis={"categoryorder": "total ascending"}
                )

                charts.append(dbc.Col([dcc.Graph(figure=institute_fig)], md=6))
    except Exception as e:
        logger.error(f"Error creating institute chart: {e}")

    try:
        # Login activity chart
        login_data = {
            "24 Hours": stats.get("recent_logins_24h", 0),
            "7 Days": stats.get("recent_logins_7d", 0),
            "30 Days": stats.get("recent_logins_30d", 0),
            "Never": stats.get("never_logged_in", 0),
        }

        if any(login_data.values()):
            login_fig = px.bar(
                x=list(login_data.keys()),
                y=list(login_data.values()),
                title="Login Activity Distribution",
            )
            login_fig.update_layout(height=300)

            charts.append(dbc.Col([dcc.Graph(figure=login_fig)], md=6))
    except Exception as e:
        logger.error(f"Error creating login chart: {e}")

    # If no charts were created, show a message
    if not charts:
        charts = [
            dbc.Col([dbc.Alert("No data available for charts", color="info")], md=12)
        ]

    # Arrange charts in rows
    chart_rows = []
    for i in range(0, len(charts), 2):
        row_charts = charts[i : i + 2]
        chart_rows.append(dbc.Row(row_charts, className="mb-3"))

    return html.Div(chart_rows)


def create_statistics_cards(stats: Dict[str, Any]) -> html.Div:
    """Create enhanced statistics cards."""
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("total_users", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "Total Users",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=2,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                f"{stats.get('active_users', 0)}/"
                                                f"{stats.get('inactive_users', 0)}",
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "Active/Inactive",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=2,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("admin_users", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "Admins",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=2,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("researcher_users", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "Researchers",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=2,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("recent_logins_7d", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "7-Day Logins",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=2,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("never_logged_in", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "Never Logged In",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=2,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("helmholtz_users", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "Helmholtz Users",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("basic_auth_users", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "Basic Auth Users",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H3(
                                                stats.get("oauth_with_basic_users", 0),
                                                className="text-center mb-0",
                                            ),
                                            html.P(
                                                "OAuth + Basic",
                                                className="text-center mb-0 small",
                                            ),
                                        ]
                                    )
                                ],
                                style=STAT_CARD_STYLE,
                            )
                        ],
                        md=3,
                    ),
                ]
            ),
        ]
    )


def create_export_data(users_data: List[Dict]) -> pd.DataFrame:
    """Create a pandas DataFrame from users data for export."""
    if not users_data:
        return pd.DataFrame()

    # Create DataFrame
    df = pd.DataFrame(users_data)

    # Ensure we have all the columns we want to export
    export_columns = [
        "username",
        "email",
        "name",
        "provider",
        "roles",
        "is_active",
        "institute",
        "last_login",
        "login_count",
        "created_at",
    ]

    # Reorder columns and fill missing ones
    for col in export_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[export_columns]

    # Clean up data for export
    df["is_active"] = df["is_active"].replace({"✓": "Yes", "✗": "No"})

    # Add export metadata
    df.index = range(1, len(df) + 1)
    df.index.name = "Row"

    return df


def generate_csv_download(users_data: List[Dict]) -> str:
    """Generate CSV file and return as base64 encoded string."""
    try:
        df = create_export_data(users_data)

        if df.empty:
            return ""

        # Create CSV in memory
        csv_buffer = io.StringIO()

        # Add metadata header
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        csv_buffer.write("# ASSAS Data Hub - User Export\n")
        csv_buffer.write(f"# Generated on: {timestamp}\n")
        csv_buffer.write(f"# Total users: {len(df)}\n")
        csv_buffer.write("#\n")

        # Add the data
        df.to_csv(csv_buffer, index=True)

        # Get CSV content and encode
        csv_content = csv_buffer.getvalue()
        csv_bytes = csv_content.encode("utf-8")
        csv_base64 = base64.b64encode(csv_bytes).decode("utf-8")

        return csv_base64

    except Exception as e:
        logger.error(f"Error generating CSV: {e}")
        return ""


def generate_excel_download(users_data: List[Dict]) -> str:
    """Generate Excel file and return as base64 encoded string."""
    try:
        df = create_export_data(users_data)

        if df.empty:
            return ""

        # Create Excel in memory
        excel_buffer = io.BytesIO()

        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            # Write main data
            df.to_excel(writer, sheet_name="Users", index=True)

            # Create summary sheet
            stats = get_user_stats()
            summary_data = {
                "Metric": [
                    "Total Users",
                    "Active Users",
                    "Inactive Users",
                    "Admin Users",
                    "Researcher Users",
                    "Helmholtz Users",
                    "Basic Auth Users",
                    "Recent Logins (7 days)",
                    "Never Logged In",
                ],
                "Count": [
                    stats.get("total_users", 0),
                    stats.get("active_users", 0),
                    stats.get("inactive_users", 0),
                    stats.get("admin_users", 0),
                    stats.get("researcher_users", 0),
                    stats.get("helmholtz_users", 0),
                    stats.get("basic_auth_users", 0),
                    stats.get("recent_logins_7d", 0),
                    stats.get("never_logged_in", 0),
                ],
            }

            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # Add metadata sheet
            metadata = {
                "Property": [
                    "Export Date",
                    "Export Time",
                    "Total Records",
                    "Database",
                    "Generated By",
                ],
                "Value": [
                    datetime.now().strftime("%Y-%m-%d"),
                    datetime.now().strftime("%H:%M:%S"),
                    len(df),
                    "MongoDB - assas.users",
                    "ASSAS Data Hub Admin Panel",
                ],
            }

            metadata_df = pd.DataFrame(metadata)
            metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

            # Format the Users sheet
            worksheet = writer.sheets["Users"]

            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter

                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except Exception as e:
                        logger.warning(
                            f"Error calculating max length for column "
                            f"{column_letter}: {e}."
                        )
                        # Ignore errors in cell value processing
                        pass

                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        # Get Excel content and encode
        excel_content = excel_buffer.getvalue()
        excel_base64 = base64.b64encode(excel_content).decode("utf-8")

        return excel_base64

    except Exception as e:
        logger.error(f"Error generating Excel: {e}")
        return ""


def create_add_user_modal() -> dbc.Modal:
    """Create modal for adding new users with 4 roles only."""
    return dbc.Modal(
        [
            dbc.ModalHeader(
                dbc.ModalTitle(
                    [html.I(className="fas fa-user-plus me-2"), "Add New User"]
                )
            ),
            dbc.ModalBody(
                [
                    dbc.Form(
                        [
                            # Username
                            dbc.Row(
                                [
                                    dbc.Label(
                                        "Username", html_for="new-username", width=3
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Input(
                                                id="new-username",
                                                type="text",
                                                placeholder="Enter username",
                                                required=True,
                                            ),
                                            dbc.FormText("Username must be unique"),
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                            ),
                            # Email
                            dbc.Row(
                                [
                                    dbc.Label("Email", html_for="new-email", width=3),
                                    dbc.Col(
                                        [
                                            dbc.Input(
                                                id="new-email",
                                                type="email",
                                                placeholder="Enter email address",
                                                required=True,
                                            )
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                            ),
                            # Full Name
                            dbc.Row(
                                [
                                    dbc.Label(
                                        "Full Name", html_for="new-name", width=3
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Input(
                                                id="new-name",
                                                type="text",
                                                placeholder="Enter full name",
                                            )
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                            ),
                            # Institute
                            dbc.Row(
                                [
                                    dbc.Label(
                                        "Institute", html_for="new-institute", width=3
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Input(
                                                id="new-institute",
                                                type="text",
                                                placeholder=(
                                                    "Enter institute/organization",
                                                ),
                                            )
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                            ),
                            # Authentication Provider
                            dbc.Row(
                                [
                                    dbc.Label(
                                        "Auth Provider",
                                        html_for="new-provider",
                                        width=3,
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Select(
                                                id="new-provider",
                                                options=[
                                                    {
                                                        "label": "Basic Authentication",
                                                        "value": "basic_auth",
                                                    },
                                                    {
                                                        "label": (
                                                            "Helmholtz Authentication",
                                                        ),
                                                        "value": "helmholtz",
                                                    },
                                                ],
                                                value="basic_auth",
                                            )
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                            ),
                            # Password (only for basic auth)
                            dbc.Row(
                                [
                                    dbc.Label(
                                        "Password", html_for="new-password", width=3
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Input(
                                                id="new-password",
                                                type="password",
                                                placeholder="Enter password "
                                                "(for basic auth only)",
                                            ),
                                            dbc.FormText(
                                                "Only required for basic authentication"
                                            ),
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                                id="password-row",
                            ),
                            # Roles - Simplified to 4 roles only
                            dbc.Row(
                                [
                                    dbc.Label("Roles", html_for="new-roles", width=3),
                                    dbc.Col(
                                        [
                                            dbc.Checklist(
                                                id="new-roles",
                                                options=[
                                                    {
                                                        "label": "Administrator - "
                                                        "Full system access",
                                                        "value": "admin",
                                                    },
                                                    {
                                                        "label": "Researcher - "
                                                        "Research data access",
                                                        "value": "researcher",
                                                    },
                                                    {
                                                        "label": "Curator - "
                                                        "Data curation access",
                                                        "value": "curator",
                                                    },
                                                    {
                                                        "label": "Visitor - "
                                                        "Basic visitor access",
                                                        "value": "visitor",
                                                    },
                                                ],
                                                value=["visitor"],  # Default role
                                                inline=False,
                                            )
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                            ),
                            # Active Status
                            dbc.Row(
                                [
                                    dbc.Label("Status", width=3),
                                    dbc.Col(
                                        [
                                            dbc.Switch(
                                                id="new-is-active",
                                                label="Active User",
                                                value=True,
                                            )
                                        ],
                                        width=9,
                                    ),
                                ],
                                className="mb-3",
                            ),
                        ]
                    )
                ]
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        "Cancel",
                        id="cancel-add-user",
                        className="me-2",
                        color="secondary",
                        outline=True,
                    ),
                    dbc.Button(
                        [html.I(className="fas fa-user-plus me-2"), "Add User"],
                        id="confirm-add-user",
                        color="primary",
                    ),
                ]
            ),
        ],
        id="add-user-modal",
        is_open=False,
        size="lg",
    )


def validate_new_user_data(
    username: str, email: str, provider: str, password: str, roles: List[str]
) -> List[str]:
    """Validate new user data."""
    errors = []

    if not username or len(username.strip()) < 3:
        errors.append("Username must be at least 3 characters long")

    if not email or "@" not in email:
        errors.append("Valid email address is required")

    if provider == "basic_auth" and (not password or len(password) < 6):
        errors.append("Password must be at least 6 characters for basic auth")

    if not roles:
        errors.append("At least one role must be selected")

    return errors


def create_new_user(
    username: str,
    email: str,
    name: str,
    institute: str,
    provider: str,
    password: str,
    roles: List[str],
    is_active: bool,
) -> Tuple[bool, str]:
    """Create a new user with the 4-role system."""
    try:
        user_manager = UserManager()

        # Check if username already exists
        existing_user = user_manager.get_user_by_username(username)
        if existing_user:
            return False, "Username already exists"

        # Check if email already exists
        existing_email = user_manager.get_user_by_email(email)
        if existing_email:
            return False, "Email already exists"

        # Validate roles (only allow the 4 defined roles)
        valid_roles = ["admin", "researcher", "curator", "visitor"]
        final_roles = [role for role in roles if role in valid_roles]

        if not final_roles:
            final_roles = ["visitor"]  # Default to visitor if no valid roles

        # Prepare user data (same structure as your assas_add_user.py)
        user_data = {
            "username": username.strip(),
            "email": email.strip().lower(),
            "name": name.strip() if name else username.title(),
            "provider": provider,
            "roles": final_roles,
            "is_active": is_active,
            "institute": institute.strip() if institute else "",
            # Timestamps
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "last_login": None,
            "login_count": 0,
            # Optional fields
            "avatar_url": None,
            "profile_url": None,
            "auth_method": provider,
            "entitlements": [],
            "affiliations": [],
        }

        # Add password hash for basic auth users
        if provider == "basic_auth" and password:
            user_data["basic_auth_password_hash"] = generate_password_hash(password)

        # Create user
        result = user_manager.create_user(user_data)

        if result:
            role_display = ", ".join([role.title() for role in final_roles])
            return (
                True,
                f"User {username} created successfully with roles: {role_display}",
            )
        else:
            return False, "Failed to create user in database"

    except Exception as e:
        logger.error(f"Error creating user: {e}")
        return False, f"Error creating user: {str(e)}"


def create_delete_confirmation_modal() -> dbc.Modal:
    """Create modal for confirming deletion of selected users."""
    return dbc.Modal(
        [
            dbc.ModalHeader(
                dbc.ModalTitle(
                    [
                        html.I(
                            className="fas fa-exclamation-triangle me-2 text-danger"
                        ),
                        "Confirm User Deletion",
                    ]
                )
            ),
            dbc.ModalBody(
                [
                    dbc.Alert(
                        [
                            html.H5("Warning!", className="alert-heading"),
                            html.P(
                                "This action cannot be undone. "
                                "The selected users will be permanently affected."
                            ),
                            html.Hr(),
                            html.Div(id="selected-users-info"),
                        ],
                        color="warning",
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dbc.Label("Deletion Type:"),
                                    dbc.RadioItems(
                                        id="delete-type-selected",
                                        options=[
                                            {
                                                "label": "Soft Delete "
                                                "(Deactivate users)",
                                                "value": "soft",
                                            },
                                            {
                                                "label": "Hard Delete "
                                                "(Permanently remove)",
                                                "value": "hard",
                                            },
                                        ],
                                        value="soft",
                                        inline=False,
                                    ),
                                ]
                            )
                        ]
                    ),
                ]
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        "Cancel",
                        id="cancel-delete-selected",
                        className="me-2",
                        color="secondary",
                    ),
                    dbc.Button(
                        [html.I(className="fas fa-trash me-2"), "Delete Users"],
                        id="confirm-delete-selected",
                        color="danger",
                    ),
                ]
            ),
        ],
        id="delete-confirmation-modal",
        is_open=False,
    )


# Update the layout function to include the add user functionality
@require_role("admin")
def layout() -> html.Div:
    """Enhanced admin page layout with add user functionality."""
    current_user = get_current_user()

    if not current_user or "admin" not in current_user.get("roles", []):
        return html.Div(
            [dbc.Alert("Access denied. Admin privileges required.", color="danger")]
        )

    # Get statistics and user data
    stats = get_user_stats()
    users_data = get_users_data()

    return html.Div(
        [
            # Header with Add User button
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H1(
                                [
                                    html.I(className="fas fa-users-cog me-3"),
                                    "Admin Dashboard",
                                ],
                                className="text-primary mb-0",
                            ),
                            html.P(
                                f"Welcome, {current_user.get('name', 'Admin')}!",
                                className="lead",
                            ),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            dbc.ButtonGroup(
                                [
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-user-plus me-2"),
                                            "Add User",
                                        ],
                                        id="open-add-user-modal",
                                        color="success",
                                        size="sm",
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-trash me-2"),
                                            "Delete Selected",
                                        ],
                                        id="delete-selected-btn",
                                        color="danger",
                                        size="sm",
                                        disabled=True,
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-file-csv me-2"),
                                            "Export CSV",
                                        ],
                                        id="export-csv-btn",
                                        color="success",
                                        outline=True,
                                        size="sm",
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-file-excel me-2"),
                                            "Export Excel",
                                        ],
                                        id="export-excel-btn",
                                        color="info",
                                        outline=True,
                                        size="sm",
                                    ),
                                ],
                                className="d-flex align-items-center",
                            )
                        ],
                        md=6,
                        className="text-end",
                    ),
                ],
                className="mb-4",
            ),
            # Alert for user operations
            html.Div(id="user-operation-alert"),
            # Existing statistics cards
            create_statistics_cards(stats),
            # Existing charts
            html.Hr(),
            html.H3("Analytics", className="mt-4 mb-3"),
            create_charts(stats),
            # UPDATED User Management Section with Delete Controls
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H3("User Management", className="mt-4 mb-3"),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            dbc.ButtonGroup(
                                [
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-user-plus me-2"),
                                            "Add User",
                                        ],
                                        id="open-add-user-modal",
                                        color="success",
                                        size="sm",
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-trash me-2"),
                                            "Delete Selected",
                                        ],
                                        id="delete-selected-btn",
                                        color="danger",
                                        size="sm",
                                        disabled=True,
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-file-csv me-2"),
                                            "Export CSV",
                                        ],
                                        id="export-csv-btn",
                                        color="success",
                                        outline=True,
                                        size="sm",
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(className="fas fa-file-excel me-2"),
                                            "Export Excel",
                                        ],
                                        id="export-excel-btn",
                                        color="info",
                                        outline=True,
                                        size="sm",
                                    ),
                                ],
                                className="mt-4",
                            )
                        ],
                        md=6,
                        className="text-end",
                    ),
                ]
            ),
            # Selection info
            html.Div(id="selection-info", className="mb-3"),
            # Existing export status
            html.Div(id="export-status", className="mb-3"),
            # Store a "last saved" snapshot so we can diff inline edits safely
            dcc.Store(id="users-table-original", data=users_data),
            # UPDATED User Table with Selection + Editing
            dash_table.DataTable(
                id="users-table",
                columns=[
                    # hidden technical id (used for updates)
                    {
                        "name": "_id",
                        "id": "_id",
                        "type": "text",
                        "editable": False,
                        "hideable": True,
                    },
                    {
                        "name": "Username",
                        "id": "username",
                        "type": "text",
                        "editable": True,
                        "hideable": True,
                    },
                    {
                        "name": "Email",
                        "id": "email",
                        "type": "text",
                        "editable": True,
                        "hideable": True,
                    },
                    {
                        "name": "Name",
                        "id": "name",
                        "type": "text",
                        "editable": True,
                        "hideable": True,
                    },
                    {
                        "name": "Auth Method",
                        "id": "provider",
                        "type": "text",
                        "editable": False,
                        "hideable": True,
                    },
                    {
                        "name": "Roles",
                        "id": "roles",
                        "type": "text",
                        "editable": True,
                        "hideable": True,
                    },
                    {
                        "name": "Active",
                        "id": "is_active",
                        "type": "text",
                        "editable": True,
                        "presentation": "dropdown",
                        "hideable": True,
                    },
                    {
                        "name": "Institute",
                        "id": "institute",
                        "type": "text",
                        "editable": True,
                        "hideable": True,
                    },
                    {
                        "name": "Batch",
                        "id": "batch",
                        "type": "text",
                        "editable": True,
                        "hideable": True,
                    },
                    {
                        "name": "Last Login",
                        "id": "last_login",
                        "type": "text",
                        "editable": False,
                        "hideable": True,
                    },
                    {
                        "name": "Login Count",
                        "id": "login_count",
                        "type": "numeric",
                        "editable": False,
                        "hideable": True,
                    },
                    {
                        "name": "Created",
                        "id": "created_at",
                        "type": "text",
                        "editable": False,
                        "hideable": True,
                    },
                ],
                data=users_data,
                editable=True,
                hidden_columns=["_id"],
                dropdown={
                    "is_active": {
                        "options": [
                            {"label": "Active (✓)", "value": "✓"},
                            {"label": "Inactive (✗)", "value": "✗"},
                        ]
                    }
                },
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                page_action="native",
                page_current=0,
                page_size=20,
                row_selectable="multi",
                selected_rows=[],
                style_cell={
                    "textAlign": "left",
                    "padding": "10px",
                    "fontFamily": "Arial, sans-serif",
                    "fontSize": "14px",
                    "whiteSpace": "normal",
                    "height": "auto",
                    "maxWidth": "200px",
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                },
                style_header={
                    "backgroundColor": "#f8f9fa",
                    "fontWeight": "bold",
                    "border": "1px solid #dee2e6",
                },
                style_data={"border": "1px solid #dee2e6"},
                style_data_conditional=[
                    {
                        "if": {"filter_query": "{is_active} = ✗"},
                        "backgroundColor": "#f8d7da",
                        "color": "black",
                    },
                    {
                        "if": {"filter_query": "{roles} contains admin"},
                        "backgroundColor": "#d1ecf1",
                        "color": "black",
                    },
                    {
                        "if": {"state": "selected"},
                        "backgroundColor": "#007bff",
                        "color": "white",
                    },
                ],
            ),
            # Custom property tool (adds/updates a field on selected users)
            dbc.Card(
                [
                    dbc.CardHeader("Add/Update a custom property on selected users"),
                    dbc.CardBody(
                        [
                            dbc.Alert(id="custom-prop-alert", is_open=False),
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Input(
                                            id="custom-prop-name",
                                            placeholder=(
                                                "Property name (e.g. department)",
                                            ),
                                            type="text",
                                        ),
                                        md=4,
                                    ),
                                    dbc.Col(
                                        dbc.Select(
                                            id="custom-prop-type",
                                            options=[
                                                {"label": "String", "value": "string"},
                                                {"label": "Number", "value": "number"},
                                                {
                                                    "label": "Boolean",
                                                    "value": "boolean",
                                                },
                                                {"label": "JSON", "value": "json"},
                                            ],
                                            value="string",
                                        ),
                                        md=3,
                                    ),
                                    dbc.Col(
                                        dbc.Input(
                                            id="custom-prop-value",
                                            placeholder=(
                                                'Value (JSON example: {"a": 1})',
                                            ),
                                            type="text",
                                        ),
                                        md=5,
                                    ),
                                ],
                                className="g-2",
                            ),
                            dbc.ButtonGroup(
                                [
                                    dbc.Button(
                                        "Apply to selected users",
                                        id="custom-prop-apply-btn",
                                        color="primary",
                                    ),
                                    dbc.Button(
                                        "Delete property from selected users",
                                        id="custom-prop-delete-btn",
                                        color="danger",
                                        outline=True,
                                    ),
                                ],
                                className="mt-3",
                            ),
                            html.Div(
                                (
                                    "Select one or more rows above, "
                                    "then apply the property."
                                ),
                                className="text-muted small mt-2",
                            ),
                        ]
                    ),
                ],
                style=ADMIN_CARD_STYLE,
                className="mt-3",
            ),
            # Keep all your existing modals
            create_add_user_modal(),
            # ADD DELETE CONFIRMATION MODAL
            create_delete_confirmation_modal(),
            # Hidden download components
            dcc.Download(id="download-csv"),
            dcc.Download(id="download-excel"),
            # System Information
            html.Hr(),
            html.H3("System Information", className="mt-4 mb-3"),
            dbc.Card(
                [
                    dbc.CardHeader(
                        [
                            html.I(className="fas fa-info-circle me-2"),
                            html.Strong("System Information"),
                        ]
                    ),
                    dbc.CardBody(
                        [
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            html.P(
                                                [
                                                    html.Strong("Database Status: "),
                                                    "Connected",
                                                ],
                                                className="mb-1",
                                            ),
                                            html.P(
                                                [
                                                    html.Strong("Total Users: "),
                                                    str(stats.get("total_users", 0)),
                                                ],
                                                className="mb-1",
                                            ),
                                            html.P(
                                                [
                                                    html.Strong("Authentication: "),
                                                    "Helmholtz AAI, Basic Auth",
                                                ],
                                                className="mb-1",
                                            ),
                                        ],
                                        md=6,
                                    ),
                                    dbc.Col(
                                        [
                                            html.P(
                                                [
                                                    html.Strong("Last Updated: "),
                                                    datetime.now().strftime(
                                                        "%Y-%m-%d %H:%M:%S"
                                                    ),
                                                ],
                                                className="mb-1",
                                            ),
                                            html.P(
                                                [
                                                    html.Strong("Active Users: "),
                                                    str(stats.get("active_users", 0)),
                                                ],
                                                className="mb-1",
                                            ),
                                            html.P(
                                                [
                                                    html.Strong("Admin Users: "),
                                                    str(stats.get("admin_users", 0)),
                                                ],
                                                className="mb-1",
                                            ),
                                        ],
                                        md=6,
                                    ),
                                ]
                            )
                        ]
                    ),
                ],
                style=ADMIN_CARD_STYLE,
            ),
            # Refresh data interval
            dcc.Interval(
                id="admin-interval-component", interval=60 * 1000, n_intervals=0
            ),
        ]
    )


# Add callback for export functionality
@callback(
    [
        Output("download-csv", "data"),
        Output("download-excel", "data"),
        Output("export-status", "children"),
    ],
    [Input("export-csv-btn", "n_clicks"), Input("export-excel-btn", "n_clicks")],
    prevent_initial_call=True,
)
def handle_export(
    csv_clicks: int, excel_clicks: int
) -> Tuple[Optional[Dict], Optional[Dict], html.Div]:
    """Handle export button clicks."""
    if not ctx.triggered:
        return None, None, ""

    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    try:
        # Get fresh user data
        users_data = get_users_data()

        if not users_data:
            status_msg = dbc.Alert(
                "No user data available for export.", color="warning", dismissable=True
            )
            return None, None, status_msg

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if button_id == "export-csv-btn":
            csv_base64 = generate_csv_download(users_data)

            if csv_base64:
                status_msg = dbc.Alert(
                    f"CSV export successful! "
                    f"Downloaded {len(users_data)} user records.",
                    color="success",
                    dismissable=True,
                )
                return (
                    {
                        "content": csv_base64,
                        "filename": f"assas_users_export_{timestamp}.csv",
                        "base64": True,
                        "type": "text/csv",
                    },
                    None,
                    status_msg,
                )
            else:
                status_msg = dbc.Alert(
                    "CSV export failed. Please try again.",
                    color="danger",
                    dismissable=True,
                )
                return None, None, status_msg

        elif button_id == "export-excel-btn":
            excel_base64 = generate_excel_download(users_data)

            if excel_base64:
                status_msg = dbc.Alert(
                    f"Excel export successful! "
                    f"Downloaded {len(users_data)} user records with statistics.",
                    color="success",
                    dismissable=True,
                )
                return (
                    None,
                    {
                        "content": excel_base64,
                        "filename": f"assas_users_export_{timestamp}.xlsx",
                        "base64": True,
                        "type": "application/vnd.openxmlformats-officedocument."
                        + "spreadsheetml.sheet",
                    },
                    status_msg,
                )
            else:
                status_msg = dbc.Alert(
                    "Excel export failed. Please try again.",
                    color="danger",
                    dismissable=True,
                )
                return None, None, status_msg

    except Exception as e:
        logger.error(f"Export error: {e}")
        status_msg = dbc.Alert(
            f"Export failed: {str(e)}", color="danger", dismissable=True
        )
        return None, None, status_msg

    return None, None, ""


# Register the page
dash.register_page(__name__, path="/admin", title="Admin Dashboard")


@callback(
    [Output("users-table", "columns"), Output("users-table", "hidden_columns")],
    Input("users-table", "data"),
)
def update_users_table_columns(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Auto-add columns for custom user properties.

    Any non-sensitive, non-protected keys present in the current table data will
    appear as hideable columns (and editable only if whitelisted in `_EDITABLE_COLS`).
    """
    # Start from the canonical base column set (includes the new 'batch' field).
    base = [
        {
            "name": "_id",
            "id": "_id",
            "type": "text",
            "editable": False,
            "hideable": True,
        },
        {
            "name": "Username",
            "id": "username",
            "type": "text",
            "editable": True,
            "hideable": True,
        },
        {
            "name": "Email",
            "id": "email",
            "type": "text",
            "editable": True,
            "hideable": True,
        },
        {
            "name": "Name",
            "id": "name",
            "type": "text",
            "editable": True,
            "hideable": True,
        },
        {
            "name": "Auth Method",
            "id": "provider",
            "type": "text",
            "editable": False,
            "hideable": True,
        },
        {
            "name": "Roles",
            "id": "roles",
            "type": "text",
            "editable": True,
            "hideable": True,
        },
        {
            "name": "Active",
            "id": "is_active",
            "type": "text",
            "editable": True,
            "presentation": "dropdown",
            "hideable": True,
        },
        {
            "name": "Institute",
            "id": "institute",
            "type": "text",
            "editable": True,
            "hideable": True,
        },
        {
            "name": "Batch",
            "id": "batch",
            "type": "text",
            "editable": True,
            "hideable": True,
        },
        {
            "name": "Last Login",
            "id": "last_login",
            "type": "text",
            "editable": False,
            "hideable": True,
        },
        {
            "name": "Login Count",
            "id": "login_count",
            "type": "numeric",
            "editable": False,
            "hideable": True,
        },
        {
            "name": "Created",
            "id": "created_at",
            "type": "text",
            "editable": False,
            "hideable": True,
        },
    ]

    if not rows:
        hidden = [c["id"] for c in base if c["id"] not in _DEFAULT_VISIBLE_COLS]
        return base, hidden

    base_ids = {c["id"] for c in base}

    extra: set[str] = set()
    for r in rows:
        for k in (r or {}).keys():
            if k in base_ids or k in _PROTECTED_COLS or k == "id":
                continue
            if k.startswith("_"):
                continue
            if _SENSITIVE_KEY_RE.search(k):
                continue
            extra.add(k)

    for k in sorted(extra):
        base.append(
            {
                "name": k.replace("_", " ").title(),
                "id": k,
                "type": "text",
                "editable": k in _EDITABLE_COLS,
                "hideable": True,
            }
        )

    hidden = [c["id"] for c in base if c["id"] not in _DEFAULT_VISIBLE_COLS]
    if "_id" not in hidden:
        hidden.append("_id")

    return base, hidden


@callback(
    Output("add-user-modal", "is_open"),
    [
        Input("open-add-user-modal", "n_clicks"),
        Input("cancel-add-user", "n_clicks"),
        Input("confirm-add-user", "n_clicks"),
    ],
    [State("add-user-modal", "is_open")],
    prevent_initial_call=True,
)
def toggle_add_user_modal(
    open_clicks: int, cancel_clicks: int, confirm_clicks: int, is_open: bool
) -> bool:
    """Toggle the add user modal."""
    if ctx.triggered_id == "open-add-user-modal":
        return True
    elif ctx.triggered_id in ["cancel-add-user", "confirm-add-user"]:
        return False
    return is_open


@callback(
    [
        Output("user-operation-alert", "children", allow_duplicate=True),
        Output("users-table", "data", allow_duplicate=True),
        Output("users-table-original", "data", allow_duplicate=True),
        Output("new-username", "value"),
        Output("new-email", "value"),
        Output("new-name", "value"),
        Output("new-institute", "value"),
        Output("new-password", "value"),
        Output("new-roles", "value"),
        Output("new-is-active", "value"),
    ],
    [Input("confirm-add-user", "n_clicks")],
    [
        State("new-username", "value"),
        State("new-email", "value"),
        State("new-name", "value"),
        State("new-institute", "value"),
        State("new-provider", "value"),
        State("new-password", "value"),
        State("new-roles", "value"),
        State("new-is-active", "value"),
    ],
    prevent_initial_call=True,
)
def handle_add_user(
    n_clicks: int,
    username: str,
    email: str,
    name: str,
    institute: str,
    provider: str,
    password: str,
    roles: list,
    is_active: bool,
) -> Tuple[html.Div, List[Dict], List[Dict], str, str, str, str, str, List[str], bool]:
    """Handle adding a new user."""
    if not n_clicks:
        return "", dash.no_update, dash.no_update, "", "", "", "", "", ["visitor"], True

    # Validate input
    validation_errors = validate_new_user_data(
        username, email, provider, password, roles
    )

    if validation_errors:
        alert = dbc.Alert(
            [
                html.H5("Validation Errors:", className="alert-heading"),
                html.Ul([html.Li(error) for error in validation_errors]),
            ],
            color="danger",
            dismissable=True,
        )
        return (
            alert,
            dash.no_update,
            dash.no_update,
            username,
            email,
            name,
            institute,
            password,
            roles,
            is_active,
        )

    # Create user
    success, message = create_new_user(
        username, email, name, institute, provider, password, roles, is_active
    )

    if success:
        # Success - refresh user data and clear form
        updated_users_data = get_users_data()
        alert = dbc.Alert(
            [html.I(className="fas fa-check-circle me-2"), message],
            color="success",
            dismissable=True,
        )
        return (
            alert,
            updated_users_data,
            updated_users_data,
            "",
            "",
            "",
            "",
            "",
            ["visitor"],
            True,
        )
    else:
        # Error - keep form data
        alert = dbc.Alert(
            [html.I(className="fas fa-exclamation-triangle me-2"), message],
            color="danger",
            dismissable=True,
        )
        return (
            alert,
            dash.no_update,
            dash.no_update,
            username,
            email,
            name,
            institute,
            password,
            roles,
            is_active,
        )


@callback(
    [
        Output("user-operation-alert", "children", allow_duplicate=True),
        Output("users-table", "data", allow_duplicate=True),
        Output("users-table-original", "data", allow_duplicate=True),
    ],
    Input("users-table", "data_timestamp"),
    [State("users-table", "data"), State("users-table-original", "data")],
    prevent_initial_call=True,
)
def persist_inline_user_edits(
    _ts: int,
    current_rows: List[Dict[str, Any]],
    original_rows: List[Dict[str, Any]],
) -> Tuple[Union[str, html.Div], List[Dict], List[Dict]]:
    """Persist inline edits from the users table into MongoDB.

    Uses `users-table-original` as the last-saved snapshot and applies a sanitized
    patch via `UserManager.update_user_by_id`.
    """
    if not current_rows or original_rows is None:
        raise PreventUpdate

    prev_by_id: Dict[str, Dict[str, Any]] = {}
    for r in original_rows or []:
        rid = r.get("_id") or r.get("id")
        if rid:
            prev_by_id[str(rid)] = r

    um = UserManager()
    updated_count = 0
    failed: List[str] = []

    for row in current_rows:
        user_id = row.get("_id") or row.get("id")
        if not user_id:
            continue
        user_id = str(user_id)

        prev = prev_by_id.get(user_id)
        if not prev:
            # Row not in our snapshot (e.g., refreshed list). Skip to avoid
            # accidental writes.
            continue

        patch = _sanitize_patch(row, prev)
        if not patch:
            continue

        logger.info(f"Applying patch !!! for user {user_id}: {patch}")
        saved = um.update_user_by_id(user_id, patch)
        if saved:
            updated_count += 1
        else:
            failed.append(row.get("email") or row.get("username") or user_id)

    # If nothing changed, keep the UI stable.
    if updated_count == 0 and not failed:
        raise PreventUpdate

    fresh = get_users_data()

    if failed:
        alert = dbc.Alert(
            [
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"Saved {updated_count} row(s). Failed for: {', '.join(failed)}",
            ],
            color="warning",
            dismissable=True,
        )
        return alert, fresh, fresh

    alert = dbc.Alert(
        [
            html.I(className="fas fa-check-circle me-2"),
            f"Saved {updated_count} row(s) to MongoDB.",
        ],
        color="success",
        dismissable=True,
    )
    return alert, fresh, fresh


@callback(Output("password-row", "style"), [Input("new-provider", "value")])
def toggle_password_field(provider: str) -> Dict[str, str]:
    """Show/hide password field based on provider selection."""
    if provider == "basic_auth":
        return {"display": "block"}
    else:
        return {"display": "none"}


# Callback to enable/disable delete button and show selection info
@callback(
    [Output("delete-selected-btn", "disabled"), Output("selection-info", "children")],
    [Input("users-table", "selected_rows")],
    [State("users-table", "data")],
)
def update_delete_button_and_info(
    selected_rows: List[int], table_data: List[Dict]
) -> Tuple[bool, Union[str, html.Div]]:
    """Enable delete button when users are selected and show selection info."""
    if not selected_rows:
        return True, ""  # Disable button, no info

    num_selected = len(selected_rows)
    selected_usernames = [
        table_data[i]["username"] for i in selected_rows if i < len(table_data)
    ]

    # Check if any admin users are selected
    admin_selected = []
    for i in selected_rows:
        if i < len(table_data):
            user = table_data[i]
            if "admin" in user.get("roles", ""):
                admin_selected.append(user["username"])

    info_content = []

    # Selection summary
    info_content.append(
        dbc.Alert(
            [
                html.I(className="fas fa-info-circle me-2"),
                f"{num_selected} user(s) selected: {', '.join(selected_usernames[:3])}",
                "..." if len(selected_usernames) > 3 else "",
            ],
            color="info",
            className="mb-2",
        )
    )

    # Admin warning
    if admin_selected:
        info_content.append(
            dbc.Alert(
                [
                    html.I(className="fas fa-exclamation-triangle me-2"),
                    f"Warning: {len(admin_selected)} admin user(s) selected: "
                    f"{', '.join(admin_selected)}",
                ],
                color="warning",
                className="mb-2",
            )
        )

    return False, html.Div(info_content)  # Enable button, show info


# Callback to open delete confirmation modal
@callback(
    [
        Output("delete-confirmation-modal", "is_open"),
        Output("selected-users-info", "children"),
    ],
    [
        Input("delete-selected-btn", "n_clicks"),
        Input("cancel-delete-selected", "n_clicks"),
        Input("confirm-delete-selected", "n_clicks"),
    ],
    [
        State("users-table", "selected_rows"),
        State("users-table", "data"),
        State("delete-confirmation-modal", "is_open"),
    ],
)
def toggle_delete_confirmation_modal(
    delete_clicks: int,
    cancel_clicks: int,
    confirm_clicks: int,
    selected_rows: List[int],
    table_data: List[Dict],
    is_open: bool,
) -> Tuple[bool, html.Div]:
    """Toggle delete confirmation modal and populate with selected user info."""
    if not ctx.triggered:
        return False, ""

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "delete-selected-btn" and selected_rows:
        # Open modal and show selected users
        selected_users_info = []
        admin_count = 0

        for i in selected_rows:
            if i < len(table_data):
                user = table_data[i]
                is_admin = "admin" in user.get("roles", "")
                if is_admin:
                    admin_count += 1

                user_info = html.Div(
                    [
                        html.P(
                            [
                                html.Strong(user.get("username", "N/A")),
                                f" ({user.get('email', 'N/A')})",
                                " - " + user.get("roles", "No roles"),
                                (
                                    html.Span(
                                        " [ADMIN]", className="text-danger fw-bold"
                                    )
                                    if is_admin
                                    else ""
                                ),
                            ],
                            className="mb-1",
                        )
                    ]
                )
                selected_users_info.append(user_info)

        # Add admin warning if needed
        warning_content = []
        if admin_count > 0:
            remaining_admins = get_remaining_admin_count(selected_rows, table_data)
            if remaining_admins == 0:
                warning_content.append(
                    dbc.Alert(
                        [
                            html.I(className="fas fa-ban me-2"),
                            "Cannot delete all admin users! "
                            "At least one admin must remain.",
                        ],
                        color="danger",
                        className="mt-3",
                    )
                )
            else:
                warning_content.append(
                    dbc.Alert(
                        [
                            html.I(className="fas fa-exclamation-triangle me-2"),
                            f"You are about to delete {admin_count} admin user(s). "
                            f"{remaining_admins} admin(s) will remain.",
                        ],
                        color="warning",
                        className="mt-3",
                    )
                )

        users_info_content = html.Div(
            [
                html.P(
                    f"Selected {len(selected_rows)} user(s) for deletion:",
                    className="fw-bold",
                ),
                html.Div(selected_users_info),
                html.Div(warning_content),
            ]
        )

        return True, users_info_content

    elif trigger_id in ["cancel-delete-selected", "confirm-delete-selected"]:
        return False, ""

    return is_open, ""


def get_remaining_admin_count(
    selected_rows: List[int],  # Selected row indices
    table_data: List[Dict],  # Full user data from the table
) -> int:
    """Calculate how many admin users will remain after deletion."""
    total_admins = sum(1 for user in table_data if "admin" in user.get("roles", ""))
    selected_admins = sum(
        1
        for i in selected_rows
        if i < len(table_data) and "admin" in table_data[i].get("roles", "")
    )
    return total_admins - selected_admins


# Callback to handle the actual deletion
@callback(
    [
        Output("user-operation-alert", "children", allow_duplicate=True),
        Output("users-table", "data", allow_duplicate=True),
        Output("users-table-original", "data", allow_duplicate=True),
        Output("users-table", "selected_rows"),
    ],
    [Input("confirm-delete-selected", "n_clicks")],
    [
        State("users-table", "selected_rows"),
        State("users-table", "data"),
        State("delete-type-selected", "value"),
    ],
    prevent_initial_call=True,
)
def delete_selected_users(
    n_clicks: int, selected_rows: List[int], table_data: List[Dict], delete_type: str
) -> Tuple[Union[str, html.Div], List[Dict], List[Dict], List[int]]:
    """Delete the selected users."""
    if not n_clicks or not selected_rows:
        return "", dash.no_update, dash.no_update, []

    try:
        user_manager = UserManager()

        # Check admin constraint
        remaining_admins = get_remaining_admin_count(selected_rows, table_data)
        selected_admin_count = sum(
            1
            for i in selected_rows
            if i < len(table_data) and "admin" in table_data[i].get("roles", "")
        )

        if remaining_admins == 0 and selected_admin_count > 0:
            alert = dbc.Alert(
                [
                    html.I(className="fas fa-ban me-2"),
                    "Cannot delete all admin users! "
                    "At least one admin must remain in the system.",
                ],
                color="danger",
                dismissable=True,
            )
            return alert, dash.no_update, dash.no_update, selected_rows

        # Perform deletions
        successful_deletions = []
        failed_deletions = []

        for i in selected_rows:
            if i < len(table_data):
                user = table_data[i]
                username = user.get("username")
                user_id = user.get("_id") or user.get("id")

                try:
                    if delete_type == "soft":
                        success = user_manager.soft_delete_user(str(user_id))
                    else:
                        success = user_manager.delete_user(str(user_id))

                    if success:
                        successful_deletions.append(username)
                    else:
                        failed_deletions.append(username)

                except Exception as e:
                    logger.error(f"Error deleting user {username}: {e}")
                    failed_deletions.append(username)

        # Create result message
        alert_content = []

        if successful_deletions:
            action_word = "deactivated" if delete_type == "soft" else "deleted"
            alert_content.append(
                dbc.Alert(
                    [
                        html.I(className="fas fa-check-circle me-2"),
                        f"Successfully {action_word} {len(successful_deletions)} ",
                        "user(s): ",
                        ", ".join(successful_deletions),
                    ],
                    color="success",
                    dismissable=True,
                )
            )

        if failed_deletions:
            alert_content.append(
                dbc.Alert(
                    [
                        html.I(className="fas fa-exclamation-triangle me-2"),
                        f"Failed to delete {len(failed_deletions)} user(s): ",
                        ", ".join(failed_deletions),
                    ],
                    color="danger",
                    dismissable=True,
                )
            )

        # Refresh user data
        updated_users_data = get_users_data()

        return (
            html.Div(alert_content),
            updated_users_data,
            updated_users_data,
            [],
        )  # Clear selection

    except Exception as e:
        logger.error(f"Error during bulk user deletion: {e}")
        alert = dbc.Alert(
            [
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"Error during deletion: {str(e)}",
            ],
            color="danger",
            dismissable=True,
        )
        return alert, dash.no_update, dash.no_update, selected_rows


@callback(
    [
        Output("custom-prop-alert", "children"),
        Output("custom-prop-alert", "color"),
        Output("custom-prop-alert", "is_open"),
        Output("users-table", "data", allow_duplicate=True),
        Output("users-table-original", "data", allow_duplicate=True),
    ],
    Input("custom-prop-apply-btn", "n_clicks"),
    [
        State("custom-prop-name", "value"),
        State("custom-prop-type", "value"),
        State("custom-prop-value", "value"),
        State("users-table", "selected_rows"),
        State("users-table", "data"),
    ],
    prevent_initial_call=True,
)
def apply_custom_property(
    _n: int,
    prop_name: str,
    prop_type: str,
    prop_value: object,
    selected_rows: List[int],
    table_data: List[Dict],
) -> Tuple[html.Div, str, bool, List[Dict], List[Dict]]:
    """Add/update a custom property on selected users using update_user_by_id()."""
    if not selected_rows:
        return (
            "Select at least one user row first.",
            "warning",
            True,
            dash.no_update,
            dash.no_update,
        )

    err = _validate_custom_field_name(prop_name or "")
    if err:
        return err, "warning", True, dash.no_update, dash.no_update

    try:
        parsed_value = _parse_custom_value(prop_value, prop_type or "string")
    except Exception as ve:
        return (
            f"Invalid value for type '{prop_type}': {ve}",
            "warning",
            True,
            dash.no_update,
            dash.no_update,
        )

    um = UserManager()
    applied = 0
    failed: List[str] = []

    for i in selected_rows:
        if i < 0 or i >= len(table_data):
            continue
        row = table_data[i]
        user_id = row.get("id") or row.get("_id")
        label = row.get("email") or row.get("username") or str(user_id)
        if not user_id:
            failed.append(label)
            continue
        logger.info(
            f"Applying custom property for user !!!! {user_id}: "
            f"{prop_name.strip()} = {parsed_value} ({prop_type})"
        )
        updated = um.update_user_by_id(str(user_id), {prop_name.strip(): parsed_value})
        if updated:
            applied += 1
        else:
            failed.append(label)

    fresh = get_users_data()

    if failed:
        return (
            f"Applied to {applied} user(s). Failed for: {', '.join(failed)}",
            "warning",
            True,
            fresh,
            fresh,
        )

    return (
        f"Property '{prop_name.strip()}' applied to {applied} user(s).",
        "success",
        True,
        fresh,
        fresh,
    )


@callback(
    [
        Output("custom-prop-alert", "children", allow_duplicate=True),
        Output("custom-prop-alert", "color", allow_duplicate=True),
        Output("custom-prop-alert", "is_open", allow_duplicate=True),
        Output("users-table", "data", allow_duplicate=True),
        Output("users-table-original", "data", allow_duplicate=True),
    ],
    Input("custom-prop-delete-btn", "n_clicks"),
    [
        State("custom-prop-name", "value"),
        State("users-table", "selected_rows"),
        State("users-table", "data"),
    ],
    prevent_initial_call=True,
)
def delete_custom_property(
    _n: int,
    prop_name: str,
    selected_rows: List[int],
    table_data: List[Dict],
) -> Tuple[html.Div, str, bool, List[Dict], List[Dict]]:
    """Remove a custom property from selected users (MongoDB $unset)."""
    if not selected_rows:
        return (
            "Select at least one user row first.",
            "warning",
            True,
            dash.no_update,
            dash.no_update,
        )

    err = _validate_custom_field_name(prop_name or "")
    if err:
        return err, "warning", True, dash.no_update, dash.no_update

    field = prop_name.strip()

    um = UserManager()
    removed = 0
    failed: List[str] = []
    succeeded_rows: List[int] = []

    for i in selected_rows:
        if i < 0 or i >= len(table_data):
            continue
        row = table_data[i]
        user_id = row.get("id") or row.get("_id")
        label = row.get("email") or row.get("username") or str(user_id)
        if not user_id:
            failed.append(label)
            continue

        ok = um.unset_user_field_by_id(str(user_id), field)
        if ok:
            removed += 1
            succeeded_rows.append(i)
        else:
            failed.append(label)

    # Update current table rows immediately so the column can disappear without waiting
    # for a full refresh.
    updated_rows: List[Dict[str, Any]] = [dict(r) for r in (table_data or [])]
    for i in succeeded_rows:
        if 0 <= i < len(updated_rows):
            updated_rows[i].pop(field, None)

    if failed:
        return (
            f"Removed '{field}' for {removed} user(s). Failed for: {', '.join(failed)}",
            "warning",
            True,
            updated_rows,
            updated_rows,
        )

    return (
        f"Removed '{field}' for {removed} user(s).",
        "success",
        True,
        updated_rows,
        updated_rows,
    )
