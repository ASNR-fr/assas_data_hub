"""Details template page for displaying metadata of a report.

This page retrieves a report by its ID and displays its metadata in a table format.
"""

import dash
import dash_bootstrap_components as dbc
import logging
import re

from typing import Any, Dict
from bson import ObjectId
from datetime import date, datetime, timezone
from uuid import UUID
from flask import current_app as app
from dash import html, Input, Output, State, callback, dcc

from assasdb import AssasDatabaseManager, AssasDatabaseHandler
from pymongo import MongoClient

from flask_app import get_mongo_client
from ..components import content_style
from ...utils.url_utils import get_base_url
from ...auth_utils import get_current_user

logger = logging.getLogger("assas_app")

dash.register_page(__name__, path_template="/details/<report_id>")


def _split_datetime_for_widgets(raw: object) -> tuple[str, str]:
    """Split datetime (YYYY-MM-DD, HH:MM:SS) for DatePickerSingle."""
    s = _to_datetime_local_value(raw)  # 'YYYY-MM-DDTHH:MM'
    if not s or "T" not in s:
        return "", ""
    d, t = s.split("T", 1)
    t = (t or "").strip()
    if len(t) == 5:  # HH:MM -> HH:MM:SS
        t = f"{t}:00"
    return d, (t[:8] if len(t) >= 8 else t)


def _combine_date_time(date_value: str | None, time_value: str | None) -> str | None:
    """Combine date+time into 'YYYY-MM-DDTHH:MM:SS' (or None if incomplete/invalid).

    Accepts HH:MM or HH:MM:SS.
    """
    d = (date_value or "").strip()
    t = (time_value or "").strip()
    if not d or not t:
        return None
    if len(t) == 5:  # HH:MM
        t = f"{t}:00"
    if len(t) != 8:
        return None
    return f"{d}T{t}"


def _parse_user_datetime(value: str | None) -> datetime | None:
    """Parse user-provided datetime string.

    Accepts:
      - datetime-local: 'YYYY-MM-DDTHH:MM' or 'YYYY-MM-DDTHH:MM:SS'
      - ISO:           'YYYY-MM-DDTHH:MM:SS', optionally with 'Z' or offset
      - Date only:     'YYYY-MM-DD' (treated as midnight)

    Returns a datetime (tz-aware if offset provided), or None if invalid.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # support trailing 'Z'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        # datetime.fromisoformat supports 'YYYY-MM-DD',
        # 'YYYY-MM-DDTHH:MM', offsets, etc.
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _to_datetime_local_value(raw: object) -> str:
    """Convert stored values (string/datetime) into a value suitable.

    For <input type="datetime-local"> => 'YYYY-MM-DDTHH:MM'.
    """
    if raw is None:
        return ""
    if isinstance(raw, datetime):
        dt = raw
    else:
        dt = _parse_user_datetime(str(raw))
        if dt is None:
            return ""

    # datetime-local should not include timezone; drop tz if present
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt.strftime("%Y-%m-%dT%H:%M")


def _created_at_display(document: dict) -> str:
    corrected = document.get("system_date_corrected")
    original = document.get("system_date")

    if corrected and original and str(corrected) != str(original):
        return f"{corrected} (original: {original})"
    return str(corrected or original or "")


def _to_json_safe(value: object) -> object:
    """Recursively convert values to JSON-serializable primitives."""
    if value is None:
        return None

    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    # handle numpy scalars etc. without importing numpy
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return _to_json_safe(value.item())
        except Exception:
            return str(value)

    if isinstance(value, dict):
        return {str(k): _to_json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [_to_json_safe(v) for v in value]

    # primitives
    if isinstance(value, (str, int, float, bool)):
        return value

    # fallback for any other custom types
    return str(value)


def serialize_document(document: dict) -> dict:
    """Convert MongoDB document to JSON-serializable format (recursive)."""
    if not document:
        return {}
    return _to_json_safe(document)


def meta_general_info_table(document: dict) -> dbc.Table:
    """Generate a table displaying general metadata information from the document."""
    general_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("NetCDF4 Dataset Attribute", style={"width": "30%"}),
                    html.Th("Value", style={"width": "70%"}),
                ]
            )
        )
    ]

    general_body = [
        html.Tbody(
            [
                html.Tr(
                    [
                        html.Td("Title", style={"width": "30%"}),
                        html.Td(
                            document.get("meta_title", ""),
                            style={
                                "width": "70%",
                                "wordWrap": "break-word",
                                "whiteSpace": "normal",
                            },
                        ),
                    ]
                ),
                html.Tr(
                    [
                        html.Td(
                            [
                                "Technical Name",
                                html.I(
                                    className="fas fa-circle-info ms-2",
                                    id="tt-tech-name-general-target",
                                    style={"color": "#6c757d"},
                                ),
                                dbc.Tooltip(
                                    children="Technical Name is the same as the “Name” shown in the main database view.",  # noqa: E501
                                    target="tt-tech-name-general-target",
                                    placement="top",
                                    trigger="hover focus",
                                    autohide=False,
                                    delay={"show": 200, "hide": 400},
                                ),
                            ],
                            style={"width": "30%"},
                        ),
                        html.Td(
                            document.get("meta_name", ""),
                            style={
                                "width": "70%",
                                "wordWrap": "break-word",
                                "whiteSpace": "normal",
                            },
                        ),
                    ]
                ),
                html.Tr(
                    [
                        html.Td("Description", style={"width": "30%"}),
                        html.Td(
                            document.get("meta_description", ""),
                            style={
                                "width": "70%",
                                "wordWrap": "break-word",
                                "whiteSpace": "normal",
                            },
                        ),
                    ]
                ),
                html.Tr(
                    [
                        html.Td("Created At", style={"width": "30%"}),
                        html.Td(
                            _created_at_display(document),
                            style={
                                "width": "70%",
                                "wordWrap": "break-word",
                                "whiteSpace": "normal",
                            },
                        ),
                    ]
                ),
            ]
        )
    ]

    table = general_header + general_body

    return dbc.Table(
        table,
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="mb-4",
        style={"tableLayout": "fixed"},
    )


def meta_data_variables_table(document: dict) -> dbc.Table:
    """Generate a table displaying data variables metadata."""
    data_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("NetCDF4 Variable Name", style={"width": "25%"}),
                    html.Th("Domain", style={"width": "25%"}),
                    html.Th("Dimensions", style={"width": "25%"}),
                    html.Th("Shape", style={"width": "25%"}),
                ]
            )
        )
    ]

    meta_data_variables = document.get("meta_data_variables")

    if meta_data_variables is None or len(meta_data_variables) == 0:
        # Return empty table or placeholder
        data_body = [
            html.Tbody(
                [
                    html.Tr(
                        [
                            html.Td(
                                "No data variables available",
                                colSpan=4,
                                style={"textAlign": "center", "fontStyle": "italic"},
                            )
                        ]
                    )
                ]
            )
        ]
        table = data_header + data_body
        return dbc.Table(
            table,
            striped=True,
            bordered=True,
            hover=True,
            responsive=True,
            className="mb-4",
            style={"tableLayout": "fixed"},
        )

    data_meta = []
    for meta_data in meta_data_variables:
        logger.debug(f"meta_data entry: {meta_data}")
        data_meta.append(
            html.Tr(
                [
                    html.Td(
                        meta_data.get("name", ""),
                        style={
                            "width": "25%",
                            "wordWrap": "break-word",
                            "whiteSpace": "normal",
                        },
                    ),
                    html.Td(
                        meta_data.get("domain", ""),
                        style={
                            "width": "25%",
                            "wordWrap": "break-word",
                            "whiteSpace": "normal",
                        },
                    ),
                    html.Td(
                        meta_data.get("dimensions", ""),
                        style={
                            "width": "25%",
                            "wordWrap": "break-word",
                            "whiteSpace": "normal",
                        },
                    ),
                    html.Td(
                        meta_data.get("shape", ""),
                        style={
                            "width": "25%",
                            "wordWrap": "break-word",
                            "whiteSpace": "normal",
                        },
                    ),
                ]
            )
        )

    data_body = [html.Tbody(data_meta)]

    table = data_header + data_body

    return dbc.Table(
        table,
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="mb-4",
        style={"tableLayout": "fixed"},
    )


def meta_info_table(document: dict) -> dbc.Table:
    """Generate a table displaying metadata information from the document."""
    general_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("NetCDF4 Dataset Attribute", style={"width": "30%"}),
                    html.Th("Value", style={"width": "70%"}),
                ]
            )
        )
    ]

    general_body = [
        html.Tbody(
            [
                html.Tr([html.Td("Name"), html.Td(document.get("meta_name", ""))]),
                html.Tr(
                    [
                        html.Td("Description"),
                        html.Td(document.get("meta_description", "")),
                    ]
                ),
            ]
        )
    ]

    data_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("NetCDF4 Variable Name", style={"width": "25%"}),
                    html.Th("Domain", style={"width": "25%"}),
                    html.Th("Dimensions", style={"width": "25%"}),
                    html.Th("Shape", style={"width": "25%"}),
                ]
            )
        )
    ]

    meta_data_variables = document.get("meta_data_variables")

    if meta_data_variables is None:
        table = general_header + general_body
        return dbc.Table(
            table,
            striped=True,
            bordered=True,
            hover=True,
            responsive=True,
            className="mb-4",
        )

    data_meta = []
    for meta_data in meta_data_variables:
        logger.debug(f"meta_data entry: {meta_data}")
        data_meta.append(
            html.Tr(
                [
                    html.Td(meta_data.get("name", "")),
                    html.Td(meta_data.get("domain", "")),
                    html.Td(meta_data.get("dimensions", "")),
                    html.Td(meta_data.get("shape", "")),
                ]
            )
        )

    data_body = [html.Tbody(data_meta)]

    table = general_header + general_body + data_header + data_body

    return dbc.Table(
        table,
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="mb-4",
    )


def _format_upload_value(value: object) -> str:
    """Best-effort formatting for values shown in the Upload Information table."""
    if value is None:
        return ""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (list, tuple)):
        # archive_paths is typically a list
        return "\n".join([_format_upload_value(v) for v in value])
    if isinstance(value, dict):
        # Keep compact (avoid dumping huge nested structures)
        items = list(value.items())
        return ", ".join([f"{k}={_format_upload_value(v)}" for k, v in items[:12]]) + (
            " ..." if len(items) > 12 else ""
        )
    return str(value)


def _fetch_upload_document(report_id: str) -> dict | None:
    """Fetch upload/file information from the files collection."""
    try:
        manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )

        # Primary: the details page uses system_uuid
        doc = manager.database_handler.file_collection.find_one(
            {"system_uuid": str(report_id)}
        )
        if doc:
            return doc

        # Fallback: some flows might route by upload_uuid
        return manager.database_handler.file_collection.find_one(
            {"upload_uuid": str(report_id)}
        )
    except Exception:
        logger.exception("Failed to fetch upload document from files collection")
        return None


def meta_upload_info_table(upload_doc: dict | None) -> dbc.Table:
    """Generate a table displaying upload information from the files collection."""
    header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("Upload Attribute", style={"width": "30%"}),
                    html.Th("Value", style={"width": "70%"}),
                ]
            )
        )
    ]

    if not upload_doc:
        body = [
            html.Tbody(
                [
                    html.Tr(
                        [
                            html.Td(
                                "No upload information available",
                                colSpan=2,
                                style={"textAlign": "center", "fontStyle": "italic"},
                            )
                        ]
                    )
                ]
            )
        ]
        return dbc.Table(
            header + body,
            striped=True,
            bordered=True,
            hover=True,
            responsive=True,
            className="mb-4",
            style={"tableLayout": "fixed"},
        )

    # Only the fields you requested (and in a friendly order)
    fields = [
        ("Upload UUID", "upload_uuid"),
        ("User", "user"),
        ("Name", "name"),
        ("Description", "description"),
        ("Archive Paths", "archive_paths"),
    ]

    rows = []
    for label, key in fields:
        if key not in upload_doc:
            continue
        val = upload_doc.get(key)
        if val is None or val == "":
            continue

        rows.append(
            html.Tr(
                [
                    html.Td(label, style={"width": "30%"}),
                    html.Td(
                        _format_upload_value(val),
                        style={
                            "width": "70%",
                            "wordWrap": "break-word",
                            "whiteSpace": "pre-wrap",
                        },
                    ),
                ]
            )
        )

    if not rows:
        rows = [
            html.Tr(
                [
                    html.Td(
                        "No upload information available",
                        colSpan=2,
                        style={"textAlign": "center", "fontStyle": "italic"},
                    )
                ]
            )
        ]

    return dbc.Table(
        header + [html.Tbody(rows)],
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="mb-4",
        style={"tableLayout": "fixed"},
    )


def meta_technical_metadata_table(
    document: dict, upload_info: dict | None
) -> dbc.Table:
    """Generate a table displaying technical metadata for the dataset."""
    header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("Technical Attribute", style={"width": "30%"}),
                    html.Th("Value", style={"width": "70%"}),
                ]
            )
        )
    ]

    candidates: list[tuple[str, object]] = [
        ("Upload UUID", (upload_info or {}).get("upload_uuid")),
        ("System UUID", document.get("system_uuid") or document.get("system_id")),
        ("HDF5 Size", document.get("system_size_hdf5")),
        ("System Size", document.get("system_size")),
        (
            "Created At",
            document.get("created_at")
            or document.get("creation_date")
            or document.get("system_date")
            or document.get("system_created_at"),
        ),
    ]

    rows = []
    for label, raw_value in candidates:
        if raw_value is None or raw_value == "":
            continue
        rows.append(
            html.Tr(
                [
                    html.Td(label, style={"width": "30%"}),
                    html.Td(
                        _format_upload_value(raw_value),
                        style={
                            "width": "70%",
                            "wordWrap": "break-word",
                            "whiteSpace": "pre-wrap",
                        },
                    ),
                ]
            )
        )

    if not rows:
        rows = [
            html.Tr(
                [
                    html.Td(
                        "No technical metadata available",
                        colSpan=2,
                        style={"textAlign": "center", "fontStyle": "italic"},
                    )
                ]
            )
        ]

    return dbc.Table(
        header + [html.Tbody(rows)],
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="mb-4",
        style={"tableLayout": "fixed"},
    )


def meta_user_info_table(current_user: dict | None) -> dbc.Table:
    """Generate a table displaying information about the currently logged-in user."""
    logger.info(f"Generating user info table for user: {current_user}")
    header = [
        html.Thead(
            html.Tr(
                [
                    html.Th("User Attribute", style={"width": "30%"}),
                    html.Th("Value", style={"width": "70%"}),
                ]
            )
        )
    ]

    if not current_user:
        body = [
            html.Tbody(
                [
                    html.Tr(
                        [
                            html.Td(
                                "No user information available",
                                colSpan=2,
                                style={"textAlign": "center", "fontStyle": "italic"},
                            )
                        ]
                    )
                ]
            )
        ]
        return dbc.Table(
            header + body,
            striped=True,
            bordered=True,
            hover=True,
            responsive=True,
            className="mb-4",
            style={"tableLayout": "fixed"},
        )

    candidates: list[tuple[str, object]] = [
        ("User", current_user.get("user") or current_user.get("username")),
        ("Name", current_user.get("name")),
        ("Email", current_user.get("email")),
        ("Roles", ", ".join(current_user.get("roles", []) or [])),
        ("Subject", current_user.get("sub") or current_user.get("subject")),
    ]

    rows: list[html.Tr] = []
    for label, raw_value in candidates:
        if raw_value is None or raw_value == "":
            continue
        rows.append(
            html.Tr(
                [
                    html.Td(label, style={"width": "30%"}),
                    html.Td(
                        _format_upload_value(raw_value),
                        style={
                            "width": "70%",
                            "wordWrap": "break-word",
                            "whiteSpace": "pre-wrap",
                        },
                    ),
                ]
            )
        )

    if not rows:
        rows = [
            html.Tr(
                [
                    html.Td(
                        "No user information available",
                        colSpan=2,
                        style={"textAlign": "center", "fontStyle": "italic"},
                    )
                ]
            )
        ]

    return dbc.Table(
        header + [html.Tbody(rows)],
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="mb-4",
        style={"tableLayout": "fixed"},
    )


def layout(report_id: str | None = None) -> html.Div:
    """Layout for the details template page."""
    logger.info(f"report_id {report_id}")

    if (report_id == "none") or (report_id is None):
        return html.Div(
            [
                html.H1("Data Details"),
                html.Div("The content is generated for each dataset."),
            ],
            style=content_style(),
        )
    else:
        document = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=MongoClient(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        ).get_database_entry_by_uuid(report_id)

        upload_doc = _fetch_upload_document(str(report_id))
        logger.info(f"Fetched upload document: {upload_doc}")
        upload_doc: dict[str, str] = (
            upload_doc.get("upload_info", {}) if upload_doc else {}
        )
        logger.info(f"Using upload info: {upload_doc}")

        logger.info(f"Found document {document}")
        base_url = get_base_url()
        datacite_url = f"{base_url}/files/datacite/{report_id}"

        user_info: dict[Any, Any] = document.get("system_user_info", {})
        logger.info(f"Using user info: {user_info}")

        current_user = get_current_user()
        show_edit = "admin" in current_user.get(
            "roles", []
        ) or "curator" in current_user.get("roles", [])
        logger.info(f"Show edit section: {show_edit}")

        return html.Div(
            [
                # Hidden store for report_id
                dcc.Store(id="current-report-id", data=report_id),
                # Hidden store for document data (JSON-serializable)
                dcc.Store(
                    id="current-document-data", data=serialize_document(document)
                ),
                html.Div(
                    [
                        html.H2(
                            "Dataset Details Page",
                            id="dataset-page-title",
                            style={
                                "fontWeight": "bold",
                                "color": "#2c3e50",
                                "marginBottom": "0.5rem",
                                "fontFamily": "Arial, sans-serif",
                            },
                        ),
                        html.P(
                            f"Titel: {document.get('meta_title', '')}",
                            id="dataset-meta-title",
                            style={
                                "fontSize": "1.1rem",
                                "color": "#444",
                                "marginBottom": "0.5rem",
                            },
                        ),
                        html.P(
                            [
                                f"Technical Name: {document.get('meta_name', '')}",
                                html.I(
                                    className="fas fa-circle-info ms-2",
                                    id="tt-tech-name-header-target",
                                    style={"color": "#6c757d"},
                                ),
                                dbc.Tooltip(
                                    children="Technical Name is the same as the “Name” shown in the main database view.",  # noqa: E501
                                    target="tt-tech-name-header-target",
                                    placement="top",
                                    trigger="hover focus",
                                ),
                            ],
                            id="dataset-meta-name",
                            style={
                                "fontSize": "1.1rem",
                                "color": "#444",
                                "marginBottom": "1.5rem",
                            },
                        ),
                        dbc.Button(
                            [
                                html.I(className="fas fa-download me-2"),
                                "Show DataCite JSON",
                            ],
                            href=datacite_url,
                            color="primary",
                            outline=True,
                            external_link=True,
                            target="_blank",
                            style={"marginBottom": "1.5rem"},
                        ),
                        (
                            html.Div(
                                [
                                    dbc.Button(
                                        [
                                            html.I(
                                                className="fas fa-pen-to-square me-2"
                                            ),
                                            "Edit Metadata",
                                        ],
                                        id="toggle-edit-metadata",
                                        color="info",
                                        outline=False,
                                        style={"marginBottom": "1rem"},
                                    ),
                                    html.Div(
                                        id="edit-metadata-section",
                                        children=[],
                                        style={"display": "none"},
                                    ),
                                ]
                            )
                            if show_edit
                            else None
                        ),
                    ],
                    style={
                        "textAlign": "center",
                        "marginBottom": "2rem",
                        "backgroundColor": "#f8f9fa",
                        "padding": "2rem",
                        "borderRadius": "12px",
                        "boxShadow": "0 2px 8px rgba(0,0,0,0.07)",
                    },
                ),
                # General Information Table
                html.H4(
                    "General Information",
                    style={
                        "color": "#007bff",
                        "marginTop": "2rem",
                        "marginBottom": "1rem",
                        "fontWeight": "bold",
                    },
                ),
                html.Div(
                    id="general-info-table-container",
                    children=meta_general_info_table(document),
                ),
                # User Information (expandable, default CLOSED)
                html.Div(
                    [
                        html.H4(
                            "User Information",
                            style={
                                "color": "#007bff",
                                "margin": "0",
                                "fontWeight": "bold",
                            },
                        ),
                        dbc.Button(
                            [
                                "Show",
                                html.I(className="fas fa-chevron-down ms-2"),
                            ],
                            id="toggle-user-info",
                            color="link",
                            className="p-0",
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "space-between",
                        "marginTop": "2rem",
                        "marginBottom": "1rem",
                    },
                ),
                dbc.Collapse(
                    html.Div(
                        id="user-info-table-container",
                        children=meta_user_info_table(user_info),
                    ),
                    id="user-info-collapse",
                    is_open=False,
                ),
                # Upload Information (expandable, default CLOSED)
                html.Div(
                    [
                        html.H4(
                            "Upload Information",
                            style={
                                "color": "#007bff",
                                "margin": "0",
                                "fontWeight": "bold",
                            },
                        ),
                        dbc.Button(
                            [
                                "Show",
                                html.I(className="fas fa-chevron-down ms-2"),
                            ],
                            id="toggle-upload-info",
                            color="link",
                            className="p-0",
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "space-between",
                        "marginTop": "2rem",
                        "marginBottom": "1rem",
                    },
                ),
                dbc.Collapse(
                    html.Div(
                        id="upload-info-table-container",
                        children=meta_upload_info_table(upload_doc),
                    ),
                    id="upload-info-collapse",
                    is_open=False,
                ),
                # Technical Meta Data (expandable, default CLOSED)
                html.Div(
                    [
                        html.H4(
                            "Technical Meta Data",
                            style={
                                "color": "#007bff",
                                "margin": "0",
                                "fontWeight": "bold",
                            },
                        ),
                        dbc.Button(
                            [
                                "Show",
                                html.I(className="fas fa-chevron-down ms-2"),
                            ],
                            id="toggle-technical-meta",
                            color="link",
                            className="p-0",
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "space-between",
                        "marginTop": "2rem",
                        "marginBottom": "1rem",
                    },
                ),
                dbc.Collapse(
                    html.Div(
                        id="technical-meta-table-container",
                        children=meta_technical_metadata_table(document, upload_doc),
                    ),
                    id="technical-meta-collapse",
                    is_open=False,
                ),
                # Data Variables Table
                html.H4(
                    "NetCDF4 Data Variables",
                    style={
                        "color": "#007bff",
                        "marginTop": "2rem",
                        "marginBottom": "1rem",
                        "fontWeight": "bold",
                    },
                ),
                html.Div(
                    id="data-variables-table-container",
                    children=meta_data_variables_table(document),
                ),
            ],
            style={
                **content_style(),
                "maxWidth": "900px",
                "margin": "2rem auto",
                "backgroundColor": "#fff",
                "borderRadius": "16px",
                "boxShadow": "0 4px 24px rgba(0,0,0,0.08)",
                "padding": "2.5rem 2rem",
            },
        )


@callback(
    Output("edit-metadata-section", "style"),
    Output("edit-metadata-section", "children"),
    Input("toggle-edit-metadata", "n_clicks"),
    State("edit-metadata-section", "style"),
    State("current-document-data", "data"),
    prevent_initial_call=True,
)
def toggle_edit_metadata(
    n_clicks: int, current_style: dict, document: dict
) -> tuple[dict, list]:
    """Toggle the visibility of the edit metadata form."""
    if n_clicks is None:
        return {"display": "none"}, []

    if current_style.get("display") == "none":
        created_at_default = document.get("system_date_corrected") or document.get(
            "system_date"
        )
        created_at_date_default, created_at_time_default = _split_datetime_for_widgets(
            created_at_default
        )

        created_at_enabled_default = False

        return (
            {"display": "block"},
            [
                dbc.Form(
                    [
                        dbc.Label("Title (max 100 characters)"),
                        dbc.Input(
                            id="edit-meta-title",
                            type="text",
                            value=document.get("meta_title", ""),
                            placeholder="Enter dataset title",
                            maxLength=100,
                        ),
                        html.Small(
                            (
                                f"{len(document.get('meta_title', '') or '')} "
                                f"/ 100 characters"
                            ),
                            id="title-char-count",
                            className="text-muted",
                            style={"display": "block", "marginBottom": "1rem"},
                        ),
                        dbc.Label("Technical Name (max 50 characters)"),
                        dbc.Input(
                            id="edit-meta-name",
                            type="text",
                            value=document.get("meta_name", ""),
                            placeholder="Enter technical dataset name",
                            maxLength=50,
                        ),
                        html.Small(
                            f"{len(document.get('meta_name', ''))} / 50 characters",
                            id="name-char-count",
                            className="text-muted",
                            style={"display": "block", "marginBottom": "1rem"},
                        ),
                        dbc.Label(
                            "Description (max 200 characters)",
                            style={"marginTop": "1rem"},
                        ),
                        dbc.Textarea(
                            id="edit-meta-description",
                            style={"height": "100px"},
                            value=document.get("meta_description", ""),
                            placeholder="Enter dataset description",
                            maxLength=200,
                        ),
                        html.Small(
                            (
                                f"{len(document.get('meta_description', ''))} "
                                f"/ 200 characters"
                            ),
                            id="desc-char-count",
                            className="text-muted",
                            style={"display": "block", "marginBottom": "1rem"},
                        ),
                        # Created At correction (OPTIONAL via checkbox) +
                        # calendar + clock (with seconds)
                        dbc.Label("Created At (correction)"),
                        dbc.Checklist(
                            id="edit-created-at-enabled",
                            options=[
                                {
                                    "label": "Enable Created At correction",
                                    "value": "enabled",
                                }
                            ],
                            value=[],
                            switch=True,
                            className="mb-2",
                        ),
                        html.Div(
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dcc.DatePickerSingle(
                                            id="edit-created-at-date",
                                            date=(created_at_date_default or None),
                                            display_format="YYYY-MM-DD",
                                            disabled=(not created_at_enabled_default),
                                        ),
                                        xs=12,
                                        md=6,
                                        className="mb-2 mb-md-0",
                                    ),
                                    dbc.Col(
                                        dbc.Input(
                                            id="edit-created-at-time",
                                            type="time",
                                            step=1,  # seconds precision
                                            value=(created_at_time_default or ""),
                                            disabled=(not created_at_enabled_default),
                                            className="assas-created-at-time",
                                        ),
                                        xs=12,
                                        md=6,
                                    ),
                                ],
                                className="g-2",
                            ),
                            className="assas-created-at-picker",
                        ),
                        html.Small(
                            "Optional. If enabled, a valid date/time is "
                            "required and must not be in the future. "
                            "Saving will NOT overwrite the original system_date; "
                            "it is stored separately as system_date_corrected.",
                            className="text-muted",
                            style={
                                "display": "block",
                                "marginBottom": "1rem",
                                "marginTop": "0.5rem",
                            },
                        ),
                        dbc.Button(
                            "Save Changes",
                            id="save-meta-btn",
                            color="success",
                            style={"marginTop": "1rem"},
                        ),
                        html.Div(id="edit-meta-feedback", style={"marginTop": "1rem"}),
                    ]
                )
            ],
        )

    return {"display": "none"}, []


# CHANGED: enable/disable date+time widgets (replaces the old edit-created-at toggler)
@callback(
    Output("edit-created-at-date", "disabled"),
    Output("edit-created-at-time", "disabled"),
    Output("edit-created-at-date", "date"),
    Output("edit-created-at-time", "value"),
    Input("edit-created-at-enabled", "value"),
    State("edit-created-at-date", "date"),
    State("edit-created-at-time", "value"),
    State("current-document-data", "data"),
    prevent_initial_call=True,
)
def toggle_created_at_input(
    enabled_value: list[str] | None,
    current_date: str | None,
    current_time: str | None,
    document: dict,
) -> tuple[bool, bool, str | None, str]:
    """Enable/disable Created At date+time inputs based on checkbox state."""
    enabled = bool(enabled_value) and ("enabled" in enabled_value)

    if enabled and (
        not (current_date or "").strip() or not (current_time or "").strip()
    ):
        created_at_default = (document or {}).get("system_date_corrected") or (
            document or {}
        ).get("system_date")
        d, t = _split_datetime_for_widgets(created_at_default)
        return False, False, (d or None), (t or "")

    return (not enabled), (not enabled), current_date, (current_time or "")


@callback(
    Output("edit-meta-feedback", "children"),
    Output("dataset-page-title", "children"),
    Output("dataset-meta-title", "children"),
    Output("dataset-meta-name", "children"),
    Output("general-info-table-container", "children"),
    Output("current-document-data", "data"),
    Input("save-meta-btn", "n_clicks"),
    State("edit-meta-title", "value"),
    State("edit-meta-name", "value"),
    State("edit-meta-description", "value"),
    State("edit-created-at-enabled", "value"),
    State("edit-created-at-date", "date"),
    State("edit-created-at-time", "value"),
    State("current-report-id", "data"),
    State("current-document-data", "data"),
    prevent_initial_call=True,
)
def save_metadata(
    n_clicks: int,
    title: str,
    name: str,
    description: str,
    created_at_enabled_value: list[str] | None,
    created_at_date: str | None,
    created_at_time: str | None,
    report_id: str,
    document: dict,
) -> tuple:
    """Save the edited metadata to the database via API."""
    if n_clicks is None:
        return (
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    try:
        # Validate input
        if not title or not title.strip():
            return (
                dbc.Alert("Title cannot be empty", color="warning"),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )

        if not name or not name.strip():
            return (
                dbc.Alert("Technical Name cannot be empty", color="warning"),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )

        if not description or not description.strip():
            return (
                dbc.Alert("Description cannot be empty", color="warning"),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )

        title_str = title.strip()
        name_str = name.strip()
        desc_str = description.strip()

        # Created At correction is OPTIONAL
        created_at_enabled = bool(created_at_enabled_value) and (
            "enabled" in created_at_enabled_value
        )
        created_at_iso: str | None = None

        if created_at_enabled:
            combined = _combine_date_time(created_at_date, created_at_time)
            if not combined:
                return (
                    dbc.Alert(
                        "Created At is required when enabled (select date and time).",
                        color="warning",
                    ),
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                )

            created_at_dt = _parse_user_datetime(combined)
            if created_at_dt is None:
                return (
                    dbc.Alert("Created At is invalid.", color="warning"),
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                )

            now = (
                datetime.now(timezone.utc)
                if created_at_dt.tzinfo is not None
                else datetime.now()
            )
            if created_at_dt > now:
                return (
                    dbc.Alert("Created At cannot be in the future.", color="warning"),
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                    dash.no_update,
                )

            created_at_iso = created_at_dt.isoformat()

        manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )

        # Uniqueness validation (case-insensitive exact match) ---
        # Exclude the current dataset by system_uuid
        exclude_self = {"system_uuid": {"$ne": str(report_id)}}

        title_re = {"$regex": f"^{re.escape(title_str)}$", "$options": "i"}
        name_re = {"$regex": f"^{re.escape(name_str)}$", "$options": "i"}

        conflict_title: Dict[str, Any] = (
            manager.database_handler.file_collection.find_one(
                {**exclude_self, "meta_title": title_re},
                {"system_uuid": 1, "meta_title": 1, "meta_name": 1},
            )
        )
        conflict_name: Dict[str, Any] = (
            manager.database_handler.file_collection.find_one(
                {**exclude_self, "meta_name": name_re},
                {"system_uuid": 1, "meta_title": 1, "meta_name": 1},
            )
        )

        if conflict_title or conflict_name:
            problems: list[str] = []

            if conflict_title:
                other_uuid = conflict_title.get("system_uuid")
                other_title = conflict_title.get("meta_title")
                other_name = conflict_title.get("meta_name")
                suffix = f" (Titel: {other_title} Technical Name: {other_name})"
                problems.append(
                    f"Title '{title_str}' is already used by "
                    f"dataset {other_uuid}.{suffix}"
                )
                logger.info(f"Title conflict with dataset {other_uuid}.{suffix}")

            if conflict_name:
                other_uuid = conflict_name.get("system_uuid")
                other_title = conflict_name.get("meta_title")
                other_name = conflict_name.get("meta_name")
                suffix = f" (Titel: {other_title} Technical Name: {other_name})"
                problems.append(
                    f"Technical Name '{name_str}' is already used by "
                    f"dataset {other_uuid}.{suffix}"
                )
                logger.info(
                    f"Technical Name conflict with dataset {other_uuid}.{suffix}"
                )

            return (
                dbc.Alert(" ".join(problems), color="warning"),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )

        update_data = {
            "meta_title": title_str,
            "meta_name": name_str,
            "meta_description": desc_str,
        }
        if created_at_enabled and created_at_iso:
            update_data["system_date_corrected"] = created_at_iso

        logger.info(
            f"Updating dataset {report_id} directly in database with: {update_data}"
        )

        result = manager.database_handler.file_collection.update_one(
            {"system_uuid": str(report_id)}, {"$set": update_data}
        )

        if result.matched_count > 0:
            updated_document = {
                **document,
                "meta_title": title_str,
                "meta_name": name_str,
                "meta_description": desc_str,
            }
            if created_at_enabled and created_at_iso:
                updated_document["system_date_corrected"] = created_at_iso

            return (
                dbc.Alert("Metadata updated successfully!", color="success"),
                dash.no_update,
                f"Titel: {title_str}",
                [
                    f"Technical Name: {name_str}",
                    html.I(
                        className="fas fa-circle-info ms-2",
                        id="tt-tech-name-header-target",
                        style={"color": "#6c757d"},
                    ),
                    dbc.Tooltip(
                        children="Technical Name is the same as the “Name” shown in the main database view.",  # noqa: E501
                        target="tt-tech-name-header-target",
                        placement="top",
                        trigger="hover focus",
                    ),
                ],
                meta_general_info_table(updated_document),
                updated_document,
            )

        return (
            dbc.Alert(
                "Failed to update dataset - no matching document found",
                color="danger",
            ),
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    except Exception as db_error:
        logger.error(f"Database error: {str(db_error)}", exc_info=True)
        return (
            dbc.Alert(f"Database error: {str(db_error)}", color="danger"),
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    except Exception as e:
        logger.error(f"Error updating metadata: {str(e)}", exc_info=True)
        return (
            dbc.Alert(f"Error updating metadata: {str(e)}", color="danger"),
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )


@callback(
    Output("user-info-collapse", "is_open"),
    Output("toggle-user-info", "children"),
    Input("toggle-user-info", "n_clicks"),
    State("user-info-collapse", "is_open"),
)
def toggle_user_info(n_clicks: int | None, is_open: bool | None) -> tuple[bool, list]:
    """Toggle the user information collapse (and sync label on initial load)."""
    is_open = bool(is_open)

    # Initial page load: just sync the label with current state
    if n_clicks is None:
        if is_open:
            return True, ["Hide", html.I(className="fas fa-chevron-up ms-2")]
        return False, ["Show", html.I(className="fas fa-chevron-down ms-2")]

    new_open = not is_open
    if new_open:
        return True, ["Hide", html.I(className="fas fa-chevron-up ms-2")]
    return False, ["Show", html.I(className="fas fa-chevron-down ms-2")]


@callback(
    Output("upload-info-collapse", "is_open"),
    Output("toggle-upload-info", "children"),
    Input("toggle-upload-info", "n_clicks"),
    State("upload-info-collapse", "is_open"),
)
def toggle_upload_info(n_clicks: int | None, is_open: bool | None) -> tuple[bool, list]:
    """Toggle the upload information collapse (and sync label on initial load)."""
    is_open = bool(is_open)

    # Initial page load: just sync the label with current state
    if n_clicks is None:
        if is_open:
            return True, ["Hide", html.I(className="fas fa-chevron-up ms-2")]
        return False, ["Show", html.I(className="fas fa-chevron-down ms-2")]

    new_open = not is_open
    if new_open:
        return True, ["Hide", html.I(className="fas fa-chevron-up ms-2")]
    return False, ["Show", html.I(className="fas fa-chevron-down ms-2")]


@callback(
    Output("technical-meta-collapse", "is_open"),
    Output("toggle-technical-meta", "children"),
    Input("toggle-technical-meta", "n_clicks"),
    State("technical-meta-collapse", "is_open"),
)
def toggle_technical_meta(
    n_clicks: int | None, is_open: bool | None
) -> tuple[bool, list]:
    """Toggle the technical metadata collapse (and sync label on initial load)."""
    is_open = bool(is_open)

    # Initial page load: just sync the label with current state
    if n_clicks is None:
        if is_open:
            return True, ["Hide", html.I(className="fas fa-chevron-up ms-2")]
        return False, ["Show", html.I(className="fas fa-chevron-down ms-2")]

    new_open = not is_open
    if new_open:
        return True, ["Hide", html.I(className="fas fa-chevron-up ms-2")]
    return False, ["Show", html.I(className="fas fa-chevron-down ms-2")]
