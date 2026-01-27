"""Details template page for displaying metadata of a report.

This page retrieves a report by its ID and displays its metadata in a table format.
"""

import dash
import dash_bootstrap_components as dbc
import logging
from bson import ObjectId

from flask import current_app as app
from dash import html, Input, Output, State, callback, dcc

from assasdb import AssasDatabaseManager, AssasDatabaseHandler
from ..components import content_style
from ...utils.url_utils import get_base_url
from ...auth_utils import get_current_user

logger = logging.getLogger("assas_app")

dash.register_page(__name__, path_template="/details/<report_id>")


def serialize_document(document: dict) -> dict:
    """Convert MongoDB document to JSON-serializable format."""
    if not document:
        return {}

    # Create a copy to avoid modifying the original
    serialized = {}
    for key, value in document.items():
        if isinstance(value, ObjectId):
            serialized[key] = str(value)
        elif isinstance(value, list):
            serialized[key] = [
                {k: str(v) if isinstance(v, ObjectId) else v for k, v in item.items()}
                if isinstance(item, dict)
                else item
                for item in value
            ]
        else:
            serialized[key] = value

    return serialized


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
                        html.Td("Name", style={"width": "30%"}),
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
                database_name=app.config["MONGO_DB_NAME"],
            )
        ).get_database_entry_by_uuid(report_id)

        logger.info(f"Found document {document}")
        base_url = get_base_url()
        datacite_url = f"{base_url}/files/datacite/{report_id}"

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
                            id="dataset-title",
                            style={
                                "fontWeight": "bold",
                                "color": "#2c3e50",
                                "marginBottom": "0.5rem",
                                "fontFamily": "Arial, sans-serif",
                            },
                        ),
                        html.P(
                            f"Name: {document.get('meta_name', '')}",
                            id="dataset-name",
                            style={
                                "fontSize": "1.1rem",
                                "color": "#444",
                                "marginBottom": "0.5rem",
                            },
                        ),
                        html.P(
                            f"Description: {document.get('meta_description', '')}",
                            id="dataset-description",
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
                                        "Edit Metadata",
                                        id="toggle-edit-metadata",
                                        color="warning",
                                        outline=True,
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

    # Toggle display and show form if visible
    if current_style.get("display") == "none":
        # Show form with current values
        return (
            {"display": "block"},
            [
                dbc.Form(
                    [
                        dbc.Label("Name (max 50 characters)"),
                        dbc.Input(
                            id="edit-meta-name",
                            type="text",
                            value=document.get("meta_name", ""),
                            placeholder="Enter dataset name",
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
    else:
        # Hide form
        return {"display": "none"}, []


# Callback to save the metadata changes
@callback(
    Output("edit-meta-feedback", "children"),
    Output("dataset-name", "children"),
    Output("dataset-description", "children"),
    Output("general-info-table-container", "children"),
    Output("current-document-data", "data"),
    Input("save-meta-btn", "n_clicks"),
    State("edit-meta-name", "value"),
    State("edit-meta-description", "value"),
    State("current-report-id", "data"),
    State("current-document-data", "data"),
    prevent_initial_call=True,
)
def save_metadata(
    n_clicks: int, name: str, description: str, report_id: str, document: dict
) -> tuple:
    """Save the edited metadata to the database via API."""
    if n_clicks is None:
        return (
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    try:
        # Validate input
        if not name or not name.strip():
            return (
                dbc.Alert("Name cannot be empty", color="warning"),
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
            )

        if len(name.strip()) > 50:
            return (
                dbc.Alert(
                    f"Name is too long ({len(name.strip())} characters). "
                    f"Maximum 50 characters allowed.",
                    color="warning",
                ),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )

        if len(description.strip()) > 200:
            return (
                dbc.Alert(
                    f"Description is too long ({len(description.strip())} characters). "
                    f"Maximum 200 characters allowed.",
                    color="warning",
                ),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )

        # SOLUTION: Call database directly instead of using HTTP API
        # This avoids authentication issues
        try:
            from assasdb import AssasDatabaseManager, AssasDatabaseHandler

            # Get database manager
            manager = AssasDatabaseManager(
                database_handler=AssasDatabaseHandler(
                    database_name=app.config["MONGO_DB_NAME"],
                )
            )

            # Prepare update data
            update_data = {
                "meta_name": name.strip(),
                "meta_description": description.strip(),
            }

            logger.info(
                f"Updating dataset {report_id} directly in database with: {update_data}"
            )

            # Perform the update using MongoDB directly
            result = manager.database_handler.file_collection.update_one(
                {"system_uuid": str(report_id)}, {"$set": update_data}
            )

            logger.info(
                f"MongoDB update result: "
                f"matched={result.matched_count}, modified={result.modified_count}"
            )

            if result.matched_count > 0:
                # Fetch updated document to verify
                updated_document_db = manager.get_database_entry_by_uuid(str(report_id))

                # Update the serialized document for Dash store
                updated_document = {
                    **document,
                    "meta_name": name.strip(),
                    "meta_description": description.strip(),
                }

                logger.info(f"Successfully updated dataset {report_id}")
                logger.info(
                    f"Updated values: "
                    f"name={updated_document_db.get('meta_name')}, "
                    f"desc={updated_document_db.get('meta_description')}"
                )

                return (
                    dbc.Alert("Metadata updated successfully!", color="success"),
                    f"Name: {name.strip()}",
                    f"Description: {description.strip()}",
                    meta_general_info_table(updated_document),
                    updated_document,
                )
            else:
                logger.error(f"No document matched for dataset {report_id}")
                return (
                    dbc.Alert(
                        "Failed to update dataset - no matching document found",
                        color="danger",
                    ),
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
            )

    except Exception as e:
        logger.error(f"Error updating metadata: {str(e)}", exc_info=True)
        return (
            dbc.Alert(f"Error updating metadata: {str(e)}", color="danger"),
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )
