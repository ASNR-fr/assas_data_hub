"""Home page for the ASSAS Data Hub Dash application.

This page provides an introduction to the ASSAS Data Hub, including a brief
description of its purpose, features, and navigation to key sections.
"""

import logging
import dash

from dash import html, dcc
from pandas import DataFrame
from flask import current_app as app

from flask_app import get_mongo_client

from ..components import content_style, encode_svg_image
from ...utils.url_utils import get_base_url
from ...database.user_manager import UserManager
from assasdb import AssasDatabaseManager, AssasDatabaseHandler

logger = logging.getLogger("assas_app")

dash.register_page(__name__, path="/home")


def hero_section_style() -> dict:
    """Define styles for the hero section.

    Returns:
        dict: CSS styles for hero section.

    """
    return {
        "backgroundColor": "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        "color": "#2c3e50",
        "padding": "4rem 2rem",
        "margin": "-2rem -0.5rem 2rem -0.5rem",
        "textAlign": "center",
        "borderRadius": "0 0 12px 12px",
        "boxShadow": "0 4px 6px rgba(0, 0, 0, 0.1)",
        "fontFamily": "Arial, sans-serif",
    }


def card_style() -> dict:
    """Define styles for feature cards.

    Returns:
        dict: CSS styles for cards.

    """
    return {
        "backgroundColor": "#ffffff",
        "border": "1px solid #e9ecef",
        "borderRadius": "12px",
        "padding": "2rem",
        "margin": "1rem 0",
        "boxShadow": "0 4px 12px rgba(0, 0, 0, 0.1)",
        "transition": "transform 0.3s ease, box-shadow 0.3s ease",
        "fontFamily": "Arial, sans-serif",
        "height": "100%",
        "position": "relative",
        ":hover": {
            "transform": "translateY(-5px)",
            "boxShadow": "0 8px 25px rgba(0, 0, 0, 0.15)",
        },
    }


def detailed_card_style() -> dict:
    """Define styles for detailed feature cards.

    Returns:
        dict: CSS styles for detailed cards.

    """
    return {
        "backgroundColor": "#ffffff",
        "border": "1px solid #e9ecef",
        "borderRadius": "12px",
        "padding": "2.5rem",
        "margin": "2rem 0",
        "boxShadow": "0 4px 15px rgba(0, 0, 0, 0.08)",
        "fontFamily": "Arial, sans-serif",
        "borderLeft": "4px solid #007bff",
    }


def stat_card_style(color: str) -> dict:
    """Define styles for statistics cards.

    Args:
        color: Primary color for the card.

    Returns:
        dict: CSS styles for stat cards.

    """
    return {
        "backgroundColor": "#ffffff",
        "border": f"2px solid {color}",
        "borderRadius": "12px",
        "padding": "2rem 1rem",
        "margin": "1rem",
        "textAlign": "center",
        "boxShadow": "0 4px 12px rgba(0, 0, 0, 0.1)",
        "transition": "transform 0.2s ease",
        "fontFamily": "Arial, sans-serif",
    }


def button_style() -> dict:
    """Define styles for action buttons.

    Returns:
        dict: CSS styles for buttons.

    """
    return {
        "backgroundColor": "#007bff",
        "color": "white",
        "padding": "12px 24px",
        "border": "none",
        "borderRadius": "8px",
        "textDecoration": "none",
        "display": "inline-block",
        "fontWeight": "600",
        "fontSize": "16px",
        "fontFamily": "Arial, sans-serif",
        "transition": "all 0.3s ease",
        "cursor": "pointer",
        "margin": "0.5rem",
        "boxShadow": "0 2px 8px rgba(0, 123, 255, 0.3)",
        ":hover": {
            "backgroundColor": "#0056b3",
            "transform": "translateY(-2px)",
            "boxShadow": "0 4px 12px rgba(0, 123, 255, 0.4)",
        },
    }


def get_user_count() -> int:
    """Fetch the total number of registered users.

    Returns:
        int: Total user count.

    """
    # Placeholder for actual database query
    user_manager = UserManager()
    all_users = user_manager.get_all_users()
    return len(all_users)


_HOME_PROJECTION_EXCLUDE = {
    "system_user_info": 0,
    "upload_info": 0,
    "system_imported_from": 0,
    "meta_description": 0,
}


def get_storage_size_binary(dataframe: DataFrame) -> str:
    """Calculate total binary storage size from the dataframe."""
    if dataframe is None or dataframe.empty or "system_size" not in dataframe.columns:
        return "0 B"

    size = dataframe["system_size"].apply(AssasDatabaseManager.convert_to_bytes).sum()
    size = AssasDatabaseManager.convert_from_bytes(size)
    return size or "0 B"


def get_storage_size_hdf5(dataframe: DataFrame) -> str:
    """Calculate total HDF5 storage size from the dataframe."""
    if (
        dataframe is None
        or dataframe.empty
        or "system_size_hdf5" not in dataframe.columns
    ):
        return "0 B"

    dataframe = dataframe.copy()
    dataframe["system_size_hdf5_bytes"] = dataframe["system_size_hdf5"].apply(
        AssasDatabaseManager.convert_to_bytes
    )
    total_size_bytes = dataframe["system_size_hdf5_bytes"].sum()
    total_size = AssasDatabaseManager.convert_from_bytes(total_size_bytes)
    return total_size or "0 B"


def get_avg_astec_vars_per_dataset(dataframe: DataFrame) -> int:
    """Calculate average number of ASTEC variables per dataset."""
    if (
        dataframe is None
        or dataframe.empty
        or "meta_data_variables" not in dataframe.columns
    ):
        return 0

    dataframe = dataframe.copy()
    dataframe["num_astec_variables"] = dataframe["meta_data_variables"].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )
    mean_val = dataframe["num_astec_variables"].mean()
    return int(mean_val) if mean_val == mean_val else 0  # NaN-safe


def layout() -> html.Div:
    """Layout for the Home page."""
    base_url = get_base_url()

    try:
        database_manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )

        # For home stats you likely want ALL docs,
        # but keep config-driven limit if needed.
        limit = int(app.config.get("HOME_STATS_LIMIT", 2000))
        batch_size = int(app.config.get("DATABASE_TABLE_BATCH_SIZE", 100))
        max_time_ms = int(app.config.get("DATABASE_TABLE_MAX_TIME_MS", 12000))

        dataframe = database_manager.get_all_database_entries_safe(
            projection=_HOME_PROJECTION_EXCLUDE,
            limit=limit,
            batch_size=batch_size,
            max_time_ms=max_time_ms,
        )
    except Exception:
        logger.exception(
            "Home page failed to query MongoDB; rendering with empty stats."
        )
        dataframe = DataFrame()

    user_count = get_user_count()
    dataset_count = len(dataframe)
    storage_binary = get_storage_size_binary(dataframe)
    storage_hdf5 = get_storage_size_hdf5(dataframe)
    avg_astec_vars = get_avg_astec_vars_per_dataset(dataframe)

    return html.Div(
        [
            # Enhanced Hero Section
            html.Div(
                [
                    html.H1(
                        "ASSAS Data Hub",
                        style={
                            "fontSize": "3.5rem",
                            "fontWeight": "700",
                            "marginBottom": "1rem",
                            "fontFamily": "Arial, sans-serif",
                            "textShadow": "2px 2px 4px rgba(0,0,0,0.3)",
                            "@media (max-width: 768px)": {"fontSize": "2.5rem"},
                            "@media (max-width: 576px)": {"fontSize": "2rem"},
                        },
                    ),
                    html.H3(
                        "Advanced Nuclear Safety Analysis Data Platform",
                        style={
                            "fontSize": "1.8rem",
                            "fontWeight": "300",
                            "marginBottom": "2rem",
                            "fontFamily": "Arial, sans-serif",
                            "opacity": "0.95",
                            "@media (max-width: 768px)": {"fontSize": "1.4rem"},
                            "@media (max-width: 576px)": {"fontSize": "1.2rem"},
                        },
                    ),
                    html.P(
                        [
                            "The premier platform for accessing comprehensive ASTEC "
                            "simulation datasets from the ",
                            html.Strong("Large Scale Data Facility (LSDF)"),
                            " at Karlsruhe Institute of Technology. Empowering nuclear "
                            "safety research through advanced data management, "
                            "visualization, and analysis tools.",
                        ],
                        style={
                            "fontSize": "1.2rem",
                            "lineHeight": "1.8",
                            "maxWidth": "900px",
                            "margin": "0 auto 2.5rem auto",
                            "fontFamily": "Arial, sans-serif",
                            "opacity": "0.9",
                            "@media (max-width: 576px)": {"fontSize": "1rem"},
                        },
                    ),
                    # Enhanced Action Buttons
                    html.Div(
                        [
                            dcc.Link(
                                [
                                    html.I(className="fas fa-database me-2"),
                                    "Explore Datasets",
                                ],
                                href=f"{base_url}/database",
                                className="btn-action",
                                style=button_style(),
                            ),
                            dcc.Link(
                                [
                                    html.I(
                                        className="fas fa-info-circle",
                                        style={"marginRight": "0.5em"},
                                    ),
                                    "About",
                                ],
                                href=f"{base_url}/about",
                                className="btn-action",
                                style={
                                    **button_style(),
                                    "backgroundColor": "#28a745",
                                    "boxShadow": "0 2px 8px rgba(40, 167, 69, 0.3)",
                                    ":hover": {
                                        "backgroundColor": "#1e7e34",
                                        "transform": "translateY(-2px)",
                                        "boxShadow": (
                                            "0 4px 12px rgba(40, 167, 69, 0.4)",
                                        ),
                                    },
                                },
                            ),
                            dcc.Link(
                                [
                                    html.I(
                                        className="fas fa-book",
                                        style={"marginRight": "0.5em"},
                                    ),
                                    "Documentation",
                                ],
                                href=f"{base_url}/documentation",
                                className="btn-action",
                                style={
                                    **button_style(),
                                    "backgroundColor": "#6c757d",
                                    "boxShadow": "0 2px 8px rgba(108, 117, 125, 0.3)",
                                    ":hover": {
                                        "backgroundColor": "#545b62",
                                        "transform": "translateY(-2px)",
                                        "boxShadow": (
                                            "0 4px 12px rgba(108, 117, 125, 0.4)"
                                        ),
                                    },
                                },
                            ),
                        ],
                        style={"marginTop": "1.5rem"},
                    ),
                ],
                className="hero-section",
                style=hero_section_style(),
            ),
            # Comprehensive Platform Statistics
            html.Div(
                [
                    html.H2(
                        "Platform Statistics & Capabilities",
                        style={
                            "textAlign": "center",
                            "color": "#2c3e50",
                            "marginBottom": "3rem",
                            "fontFamily": "Arial, sans-serif",
                            "fontSize": "2.5rem",
                        },
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H2(
                                        f"{user_count:,}",
                                        style={
                                            "color": "#007bff",
                                            "margin": "0",
                                            "fontSize": "2.5rem",
                                            "fontWeight": "bold",
                                        },
                                    ),
                                    html.P(
                                        "Registered Users",
                                        style={
                                            "margin": "0.5rem 0",
                                            "fontSize": "1.1rem",
                                            "fontWeight": "500",
                                        },
                                    ),
                                    html.Small(
                                        "Researchers with platform access",
                                        style={"color": "#6c757d"},
                                    ),
                                ],
                                className="stat-card",
                                style=stat_card_style("#007bff"),
                            ),
                            html.Div(
                                [
                                    html.H2(
                                        f"{dataset_count:,}",
                                        style={
                                            "color": "#28a745",
                                            "margin": "0",
                                            "fontSize": "2.5rem",
                                            "fontWeight": "bold",
                                        },
                                    ),
                                    html.P(
                                        "Available Datasets",
                                        style={
                                            "margin": "0.5rem 0",
                                            "fontSize": "1.1rem",
                                            "fontWeight": "500",
                                        },
                                    ),
                                    html.Small(
                                        "ASTEC simulation datasets",
                                        style={"color": "#6c757d"},
                                    ),
                                ],
                                className="stat-card",
                                style=stat_card_style("#28a745"),
                            ),
                            html.Div(
                                [
                                    html.H2(
                                        storage_binary,
                                        style={
                                            "color": "#fd7e14",
                                            "margin": "0",
                                            "fontSize": "2.2rem",
                                            "fontWeight": "bold",
                                        },
                                    ),
                                    html.P(
                                        "Binary Storage Size",
                                        style={
                                            "margin": "0.5rem 0",
                                            "fontSize": "1.1rem",
                                            "fontWeight": "500",
                                        },
                                    ),
                                    html.Small(
                                        "Binary simulation data",
                                        style={"color": "#6c757d"},
                                    ),
                                ],
                                className="stat-card",
                                style=stat_card_style("#fd7e14"),
                            ),
                            html.Div(
                                [
                                    html.H2(
                                        storage_hdf5,
                                        style={
                                            "color": "#6f42c1",
                                            "margin": "0",
                                            "fontSize": "2.2rem",
                                            "fontWeight": "bold",
                                        },
                                    ),
                                    html.P(
                                        "HDF5 Storage Size",
                                        style={
                                            "margin": "0.5rem 0",
                                            "fontSize": "1.1rem",
                                            "fontWeight": "500",
                                        },
                                    ),
                                    html.Small(
                                        "Hierarchical data format",
                                        style={"color": "#6c757d"},
                                    ),
                                ],
                                className="stat-card",
                                style=stat_card_style("#6f42c1"),
                            ),
                            html.Div(
                                [
                                    html.H2(
                                        f"{avg_astec_vars:,}",
                                        style={
                                            "color": "#20c997",
                                            "margin": "0",
                                            "fontSize": "2.2rem",
                                            "fontWeight": "bold",
                                        },
                                    ),
                                    html.P(
                                        "ASTEC Variables per Dataset",
                                        style={
                                            "margin": "0.5rem 0",
                                            "fontSize": "1.1rem",
                                            "fontWeight": "500",
                                        },
                                    ),
                                    html.Small(
                                        "Typical number of variables per dataset",
                                        style={"color": "#6c757d"},
                                    ),
                                ],
                                className="stat-card",
                                style=stat_card_style("#20c997"),
                            ),
                        ],
                        className="stats-container",
                        style={"textAlign": "center", "marginBottom": "2rem"},
                    ),
                ],
                style=content_style(),
            ),
            # Comprehensive Features Section
            html.Div(
                [
                    html.H2(
                        "Comprehensive Database — FAIR Principles",
                        style={
                            "textAlign": "center",
                            "color": "#2c3e50",
                            "marginBottom": "3rem",
                            "fontFamily": "Arial, sans-serif",
                            "fontSize": "2.5rem",
                        },
                    ),
                    # Findable
                    html.Div(
                        [
                            html.H3(
                                "🔎 Findable",
                                style={
                                    "color": "#007bff",
                                    "fontFamily": "Arial, sans-serif",
                                    "fontSize": "1.8rem",
                                    "marginBottom": "1rem",
                                },
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        [
                                            html.Strong("Rich Metadata & Indexing: "),
                                            "All datasets are described with detailed ",
                                            "metadata, including simulation parameters",
                                            ", provenance, and keywords for "
                                            "easy discovery.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong(
                                                "Advanced Search & Filtering: "
                                            ),
                                            "Multi-parameter search by reactor type, ",
                                            "accident scenario, physical phenomena, ",
                                            "and more.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong("Persistent Identifiers: "),
                                            "Each dataset is assigned a unique, ",
                                            "persistent identifier for citation ",
                                            "and reference.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "lineHeight": "1.8",
                                    "fontFamily": "Arial, sans-serif",
                                },
                            ),
                        ],
                        style=detailed_card_style(),
                    ),
                    # Accessible
                    html.Div(
                        [
                            html.H3(
                                "🌐 Accessible",
                                style={
                                    "color": "#28a745",
                                    "fontFamily": "Arial, sans-serif",
                                    "fontSize": "1.8rem",
                                    "marginBottom": "1rem",
                                },
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        [
                                            html.Strong(
                                                "Open Access for Registered Users: "
                                            ),
                                            "Datasets are available to all ",
                                            "authenticated researchers, ",
                                            "with clear access policies.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong(
                                                "Standardized Download & API: "
                                            ),
                                            "Data can be accessed via web ",
                                            "interface or RESTful API, supporting ",
                                            "batch and programmatic retrieval.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong(
                                                "Comprehensive Documentation: "
                                            ),
                                            "Guides and API docs ensure users ",
                                            "can easily access and use the data.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "lineHeight": "1.8",
                                    "fontFamily": "Arial, sans-serif",
                                },
                            ),
                        ],
                        style=detailed_card_style(),
                    ),
                    # Interoperable
                    html.Div(
                        [
                            html.H3(
                                "🔗 Interoperable",
                                style={
                                    "color": "#6f42c1",
                                    "fontFamily": "Arial, sans-serif",
                                    "fontSize": "1.8rem",
                                    "marginBottom": "1rem",
                                },
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        [
                                            html.Strong("Standard Data Formats: "),
                                            "Support for NetCDF4 and HDF5 formats ",
                                            "ensures compatibility with scientific ",
                                            "tools and workflows.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong("Controlled Vocabularies: "),
                                            "ASTEC variable naming conventions ",
                                            "and metadata standards enable ",
                                            "integration with other platforms.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong("API & SDK Integration: "),
                                            "RESTful API and Python SDK allow ",
                                            "seamless interoperability with analysis "
                                            "pipelines and external systems.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "lineHeight": "1.8",
                                    "fontFamily": "Arial, sans-serif",
                                },
                            ),
                        ],
                        style=detailed_card_style(),
                    ),
                    # Reusable
                    html.Div(
                        [
                            html.H3(
                                "♻️ Reusable",
                                style={
                                    "color": "#fd7e14",
                                    "fontFamily": "Arial, sans-serif",
                                    "fontSize": "1.8rem",
                                    "marginBottom": "1rem",
                                },
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        [
                                            html.Strong(
                                                "Clear Licensing & Usage Terms: "
                                            ),
                                            "Datasets include explicit licensing ",
                                            "and recommended usage scenarios for ",
                                            "reproducible research.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong("Rich Metadata & Provenance: "),
                                            "Detailed provenance information ",
                                            "supports data reuse and validation.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                    html.Li(
                                        [
                                            html.Strong("Community Standards: "),
                                            "Data curation follows community best "
                                            "practices for nuclear safety analysis.",
                                        ],
                                        style={
                                            "marginBottom": "0.8rem",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "lineHeight": "1.8",
                                    "fontFamily": "Arial, sans-serif",
                                },
                            ),
                        ],
                        style=detailed_card_style(),
                    ),
                ],
                style={**content_style(), "margin": "4rem 0"},
            ),
            # Technical Specifications
            html.Div(
                [
                    html.H2(
                        "Technical Specifications",
                        style={
                            "textAlign": "center",
                            "color": "#2c3e50",
                            "marginBottom": "2rem",
                            "fontFamily": "Arial, sans-serif",
                        },
                    ),
                    html.Div(
                        [
                            # Left Column: Data & Metadata
                            html.Div(
                                [
                                    html.H4(
                                        "Data Formats & Standards",
                                        style={
                                            "color": "#007bff",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.Ul(
                                        [
                                            html.Li(
                                                "NetCDF4 with CF-1.8 metadata "
                                                "conventions"
                                            ),
                                            html.Li(
                                                "HDF5 hierarchical data format support"
                                            ),
                                            html.Li(
                                                "ASTEC-specific variable naming "
                                                "standards"
                                            ),
                                            html.Li(
                                                "ISO 8601 temporal data formatting"
                                            ),
                                            html.Li(
                                                "UTF-8 encoding for all text metadata"
                                            ),
                                            html.Li(
                                                "Direct access to LSDF "
                                                "(Large Scale Data Facility) storage"
                                            ),
                                            html.Li(
                                                "Efficient binary storage for "
                                                "large simulation datasets"
                                            ),
                                            html.Li(
                                                "Automatic calculation and reporting "
                                                "of dataset sizes"
                                            ),
                                            html.Li(
                                                "Support for multi-dimensional "
                                                "ASTEC variables"
                                            ),
                                        ],
                                        style={"lineHeight": "1.8"},
                                    ),
                                    html.H4(
                                        "Metadata & FAIR Compliance",
                                        style={
                                            "color": "#28a745",
                                            "marginTop": "2rem",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.Ul(
                                        [
                                            html.Li(
                                                "Rich metadata including simulation "
                                                "parameters, provenance, and keywords"
                                            ),
                                            html.Li(
                                                "Persistent identifiers "
                                                "for all datasets"
                                            ),
                                            html.Li(
                                                "Compliance with FAIR data principles"
                                            ),
                                        ],
                                        style={"lineHeight": "1.8"},
                                    ),
                                ],
                                style={
                                    "width": "48%",
                                    "display": "inline-block",
                                    "verticalAlign": "top",
                                    "margin": "1%",
                                },
                            ),
                            # Right Column: Access, Security, Performance, Login Methods
                            html.Div(
                                [
                                    html.H4(
                                        "Security & Access Control",
                                        style={
                                            "color": "#dc3545",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.Ul(
                                        [
                                            html.Li(
                                                "OAuth 2.0 authentication framework"
                                            ),
                                            html.Li("Role-based access control (RBAC)"),
                                            html.Li(
                                                "Encrypted data transmission (TLS 1.3)"
                                            ),
                                            html.Li(
                                                "Internal logging of data requests"
                                            ),
                                            html.Li("GDPR-compliant data handling"),
                                            html.Li(
                                                [
                                                    html.Strong(
                                                        "Helmholtz AAI Login: "
                                                    ),
                                                    "Recommended for Helmholtz "
                                                    "researchers. "
                                                    "Secure single sign-on via ",
                                                    html.A(
                                                        "Helmholtz AAI",
                                                        href=str(
                                                            "https://"
                                                            + "www.helmholtz.de/"
                                                            + "en/about-us/",
                                                        ),
                                                        target="_blank",
                                                        style={
                                                            "color": "#007bff",
                                                            "textDecoration": (
                                                                "underline",
                                                            ),
                                                        },
                                                    ),
                                                    ".",
                                                ],
                                            ),
                                            html.Li(
                                                [
                                                    html.Strong("Basic Auth Login: "),
                                                    "Available for users with "
                                                    "local credentials.",
                                                ],
                                            ),
                                        ],
                                        style={"lineHeight": "1.8"},
                                    ),
                                    html.H4(
                                        "Performance & Scalability",
                                        style={
                                            "color": "#6f42c1",
                                            "marginTop": "2rem",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.Ul(
                                        [
                                            html.Li(
                                                "Optimized for large-scale, "
                                                "concurrent data access"
                                            ),
                                            html.Li(
                                                "Responsive web interface for"
                                                " dataset browsing"
                                            ),
                                            html.Li(
                                                "Scalable architecture "
                                                "supporting 100+ users"
                                            ),
                                            html.Li(
                                                "Scheduled dataset updates and curation"
                                            ),
                                        ],
                                        style={"lineHeight": "1.8"},
                                    ),
                                ],
                                style={
                                    "width": "48%",
                                    "display": "inline-block",
                                    "verticalAlign": "top",
                                    "margin": "1%",
                                },
                            ),
                        ],
                        style={
                            "@media (max-width: 768px)": {
                                "display": "block",
                            },
                        },
                    ),
                ],
                style={
                    "backgroundColor": "#f8f9fa",
                    "padding": "3rem 2rem",
                    "borderRadius": "12px",
                    "margin": "3rem 0",
                    "fontFamily": "Arial, sans-serif",
                },
            ),
            # System Overview Image
            html.Div(
                [
                    html.H2(
                        "System Architecture Overview",
                        style={
                            "textAlign": "center",
                            "color": "#2c3e50",
                            "marginBottom": "1rem",
                            "fontFamily": "Arial, sans-serif",
                        },
                    ),
                    html.P(
                        [
                            "The ASSAS Data Hub implements a modern, scalable "
                            "architecture connecting researchers to high-performance "
                            "storage systems through advanced web technologies and "
                            "optimized data pipelines.",
                        ],
                        style={
                            "textAlign": "center",
                            "color": "#5a6c7d",
                            "marginBottom": "2rem",
                            "lineHeight": "1.6",
                            "fontFamily": "Arial, sans-serif",
                            "fontSize": "1.1rem",
                        },
                    ),
                    html.Img(
                        src=encode_svg_image("assas_introduction.drawio.svg"),
                        style={
                            "maxWidth": "100%",
                            "height": "auto",
                            "width": "auto",
                            "display": "block",
                            "margin": "2rem auto",
                            "borderRadius": "12px",
                            "boxShadow": "0 8px 25px rgba(0, 0, 0, 0.15)",
                            "@media (min-width: 992px)": {
                                "maxWidth": "1000px",
                                "width": "100%",
                            },
                            "@media (max-width: 991px) and (min-width: 577px)": {
                                "maxWidth": "100%",
                                "width": "95%",
                            },
                            "@media (max-width: 576px)": {
                                "width": "100%",
                                "margin": "1rem auto",
                            },
                        },
                        alt="ASSAS Data Hub System Architecture",
                    ),
                ],
                style={"margin": "4rem 0"},
            ),
            # Enhanced Getting Started Section
            html.Div(
                [
                    html.H2(
                        "Getting Started Guide",
                        style={
                            "color": "#2c3e50",
                            "marginBottom": "2rem",
                            "fontFamily": "Arial, sans-serif",
                            "textAlign": "center",
                        },
                    ),
                    # Step-by-step guide
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.H3(
                                                "1",
                                                style={
                                                    "color": "white",
                                                    "fontSize": "2rem",
                                                    "margin": "0",
                                                },
                                            ),
                                        ],
                                        style={
                                            "backgroundColor": "#007bff",
                                            "width": "60px",
                                            "height": "60px",
                                            "borderRadius": "50%",
                                            "display": "flex",
                                            "alignItems": "center",
                                            "justifyContent": "center",
                                            "margin": "0 auto 1rem auto",
                                        },
                                    ),
                                    html.H4(
                                        "Explore Datasets",
                                        style={
                                            "color": "#007bff",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.P(
                                        [
                                            "Navigate to the Database section to ",
                                            "browse our comprehensive collection of ",
                                            html.Strong(
                                                f"{dataset_count} ASTEC datasets",
                                            ),
                                            " covering various accident scenarios.",
                                        ],
                                        style={
                                            "lineHeight": "1.6",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "textAlign": "center",
                                    "padding": "2rem",
                                    "margin": "1rem",
                                },
                            ),
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.H3(
                                                "2",
                                                style={
                                                    "color": "white",
                                                    "fontSize": "2rem",
                                                    "margin": "0",
                                                },
                                            ),
                                        ],
                                        style={
                                            "backgroundColor": "#28a745",
                                            "width": "60px",
                                            "height": "60px",
                                            "borderRadius": "50%",
                                            "display": "flex",
                                            "alignItems": "center",
                                            "justifyContent": "center",
                                            "margin": "0 auto 1rem auto",
                                        },
                                    ),
                                    html.H4(
                                        "Filter & Search",
                                        style={
                                            "color": "#28a745",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.P(
                                        [
                                            "Use our advanced multi-parameter ",
                                            "filtering system to find datasets ",
                                            "matching your specific research ",
                                            "requirements across ",
                                            html.Strong(
                                                f"{avg_astec_vars} ASTEC variables",
                                            ),
                                            ".",
                                        ],
                                        style={
                                            "lineHeight": "1.6",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "textAlign": "center",
                                    "padding": "2rem",
                                    "margin": "1rem",
                                },
                            ),
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.H3(
                                                "3",
                                                style={
                                                    "color": "white",
                                                    "fontSize": "2rem",
                                                    "margin": "0",
                                                },
                                            ),
                                        ],
                                        style={
                                            "backgroundColor": "#dc3545",
                                            "width": "60px",
                                            "height": "60px",
                                            "borderRadius": "50%",
                                            "display": "flex",
                                            "alignItems": "center",
                                            "justifyContent": "center",
                                            "margin": "0 auto 1rem auto",
                                        },
                                    ),
                                    html.H4(
                                        "Access Data",
                                        style={
                                            "color": "#dc3545",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.P(
                                        [
                                            "Download up to ",
                                            html.Strong(f"{storage_hdf5} of data"),
                                            " in hdf5 format via the ",
                                            "web interface or access them ",
                                            "programmatically via the RESTful API.",
                                        ],
                                        style={
                                            "lineHeight": "1.6",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "textAlign": "center",
                                    "padding": "2rem",
                                    "margin": "1rem",
                                },
                            ),
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.H3(
                                                "4",
                                                style={
                                                    "color": "white",
                                                    "fontSize": "2rem",
                                                    "margin": "0",
                                                },
                                            ),
                                        ],
                                        style={
                                            "backgroundColor": "#6f42c1",
                                            "width": "60px",
                                            "height": "60px",
                                            "borderRadius": "50%",
                                            "display": "flex",
                                            "alignItems": "center",
                                            "justifyContent": "center",
                                            "margin": "0 auto 1rem auto",
                                        },
                                    ),
                                    html.H4(
                                        "Analyze Results",
                                        style={
                                            "color": "#6f42c1",
                                            "marginBottom": "1rem",
                                        },
                                    ),
                                    html.P(
                                        [
                                            "Use the comprehensive metadata, ",
                                            "documentation and the ",
                                            html.Strong("NetCDF4 API"),
                                            " to perform data analysis with ",
                                            "full traceability and reproducibility.",
                                        ],
                                        style={
                                            "lineHeight": "1.6",
                                            "fontSize": "1.1rem",
                                        },
                                    ),
                                ],
                                style={
                                    "textAlign": "center",
                                    "padding": "2rem",
                                    "margin": "1rem",
                                },
                            ),
                        ],
                        style={
                            "display": "grid",
                            "gridTemplateColumns": (
                                "repeat(auto-fit, minmax(250px, 1fr))",
                            ),
                            "gap": "1rem",
                            "margin": "2rem 0",
                        },
                    ),
                    # Call to action
                    html.Div(
                        [
                            html.H3(
                                "Join the ASSAS Community",
                                style={
                                    "color": "#2c3e50",
                                    "fontFamily": "Arial, sans-serif",
                                    "marginBottom": "1.5rem",
                                },
                            ),
                            html.P(
                                [
                                    "The ASSAS Data Hub is a collaborative project ",
                                    "supported by leading partners in nuclear safety ",
                                    "research. Our platform enables secure, ",
                                    "FAIR-compliant access to high-quality ASTEC ",
                                    "simulation data for research, analysis, ",
                                    "and innovation. Whether you are a project partner",
                                    ", academic researcher, or industry expert, ",
                                    "ASSAS provides the tools and resources to ",
                                    "advance nuclear safety science.",
                                ],
                                style={
                                    "fontSize": "1.1rem",
                                    "lineHeight": "1.8",
                                    "marginBottom": "2rem",
                                    "color": "#5a6c7d",
                                },
                            ),
                            html.P(
                                [
                                    "For more information about the ASSAS project, ",
                                    "its partners, and ongoing initiatives, ",
                                    "please visit the ",
                                    html.A(
                                        "ASSAS Home Page",
                                        href="https://assas-horizon-euratom.eu/",
                                        target="_blank",
                                        style={
                                            "color": "#007bff",
                                            "textDecoration": "underline",
                                        },
                                    ),
                                    ".",
                                ],
                                style={
                                    "fontSize": "1.1rem",
                                    "lineHeight": "1.8",
                                    "marginBottom": "2rem",
                                    "color": "#5a6c7d",
                                },
                            ),
                            dcc.Link(
                                [
                                    html.I(className="fas fa-database me-2"),
                                    "Explore Datasets",
                                ],
                                href=f"{base_url}/database",
                                className="btn-action",
                                style=button_style(),
                            ),
                            dcc.Link(
                                [
                                    html.I(
                                        className="fas fa-info-circle",
                                        style={"marginRight": "0.5em"},
                                    ),
                                    "About",
                                ],
                                href=f"{base_url}/about",
                                className="btn-action",
                                style={
                                    **button_style(),
                                    "backgroundColor": "#28a745",
                                    "boxShadow": "0 2px 8px rgba(40, 167, 69, 0.3)",
                                    ":hover": {
                                        "backgroundColor": "#1e7e34",
                                        "transform": "translateY(-2px)",
                                        "boxShadow": (
                                            "0 4px 12px rgba(40, 167, 69, 0.4)",
                                        ),
                                    },
                                },
                            ),
                            dcc.Link(
                                [
                                    html.I(
                                        className="fas fa-book",
                                        style={"marginRight": "0.5em"},
                                    ),
                                    "Documentation",
                                ],
                                href=f"{base_url}/documentation",
                                className="btn-action",
                                style={
                                    **button_style(),
                                    "backgroundColor": "#6c757d",
                                    "boxShadow": "0 2px 8px rgba(108, 117, 125, 0.3)",
                                    ":hover": {
                                        "backgroundColor": "#545b62",
                                        "transform": "translateY(-2px)",
                                        "boxShadow": (
                                            "0 4px 12px rgba(108, 117, 125, 0.4)"
                                        ),
                                    },
                                },
                            ),
                        ],
                        style={
                            "textAlign": "center",
                            "marginTop": "3rem",
                            "padding": "3rem 2rem",
                            "backgroundColor": (
                                "linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%)"
                            ),
                            "borderRadius": "12px",
                            "border": "1px solid #dee2e6",
                        },
                    ),
                ],
                style={
                    "backgroundColor": "#ffffff",
                    "padding": "3rem 2rem",
                    "borderRadius": "12px",
                    "border": "1px solid #e9ecef",
                    "margin": "3rem 0",
                    "boxShadow": "0 4px 15px rgba(0, 0, 0, 0.08)",
                },
            ),
            # Contact Footer
            html.Div(
                [
                    html.Hr(),
                    html.Div(
                        [
                            html.H4(
                                "Contact & Support",
                                style={"color": "#2c3e50", "marginBottom": "1rem"},
                            ),
                            html.P(
                                [
                                    "For technical support, research collaboration, or "
                                    "questions about the ASSAS Data Hub platform:"
                                ],
                                style={"marginBottom": "1rem", "color": "#5a6c7d"},
                            ),
                            html.Div(
                                [
                                    html.I(
                                        className="fas fa-envelope me-2",
                                        style={"color": "#007bff"},
                                    ),
                                    html.A(
                                        "jonas.dressner@kit.edu",
                                        href="mailto:jonas.dressner@kit.edu",
                                        style={
                                            "color": "#007bff",
                                            "textDecoration": "none",
                                            "fontSize": "1.1rem",
                                            "fontWeight": "500",
                                        },
                                    ),
                                ],
                                style={"margin": "1rem 0"},
                            ),
                            html.P(
                                [
                                    "Jonas Dressner | ",
                                    "Karlsruhe Institute of Technology",
                                ],
                                style={
                                    "fontStyle": "italic",
                                    "color": "#6c757d",
                                    "margin": "0",
                                },
                            ),
                        ],
                        style={
                            "textAlign": "center",
                            "padding": "2rem",
                            "backgroundColor": "#f8f9fa",
                            "borderRadius": "8px",
                            "borderTop": "3px solid #007bff",
                        },
                    ),
                ],
                style={"margin": "3rem 0 1rem 0"},
            ),
        ],
        style=content_style(),
    )
