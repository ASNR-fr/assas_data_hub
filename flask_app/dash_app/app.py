"""Create a Plotly Dash app with Flask integration."""

import os
import logging
import uuid
import diskcache
import dash_bootstrap_components as dbc
import flask
import dash as ds

from dash import dash, html, Input, Output, State, dcc
from dash.long_callback import DiskcacheLongCallbackManager

from .components import encode_svg_image_hq
from flask import redirect, request, Response, session
from ..auth_utils import is_authenticated, get_current_user
from ..auth.oauth_auth import init_oauth
from ..utils.url_utils import (
    get_base_url,
    get_auth_base_url,
    get_dash_base_url,
    build_auth_url,
    build_url,
)

logger = logging.getLogger("assas_app")


def get_user_role() -> str:
    """Get the current user's role from the session."""
    user = flask.session.get("user", {})
    roles = user.get("roles", [])
    return roles[0] if roles else "visitor"


def get_allowed_pages(role: str) -> list[str]:
    """Get the list of allowed pages for a specific user role."""
    if role == "admin":
        return ["home", "database", "profile", "admin", "documentation", "about"]
    elif role == "curator":
        return ["home", "database", "profile", "documentation", "about"]
    elif role == "researcher":
        return ["home", "database", "profile", "documentation", "about"]
    elif role == "visitor":
        return ["visitor", "profile"]
    else:
        raise ValueError(f"Unknown role: {role}")


def modern_navbar_style() -> dict:
    """Define modern navbar styles.

    Returns:
        dict: CSS styles for the navbar.

    """
    return {
        "backgroundColor": "#2c3e50",  # Modern dark blue
        "borderBottom": "3px solid #3498db",  # Blue accent border
        "boxShadow": "0 4px 12px rgba(0, 0, 0, 0.15)",  # Enhanced shadow
        "fontFamily": "Arial, sans-serif",
        "fontSize": "1rem",
        "padding": "0.5rem 0",
        "position": "sticky",
        "top": "0",
        "zIndex": "1030",
        # Add transitions
        "transition": "transform 0.3s ease-in-out, opacity 0.3s ease-in-out",
        "opacity": "1",  # Default opacity
        "transform": "translateY(0)",  # Default position
    }


def top_row_style() -> dict:
    """Define top row styles for brand section.

    Returns:
        dict: CSS styles for top row.

    """
    return {
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center",
        "padding": "1rem 0",
        "borderBottom": "1px solid #34495e",
        "backgroundColor": "#2c3e50",
    }


def bottom_row_style() -> dict:
    """Define bottom row styles for navigation.

    Returns:
        dict: CSS styles for bottom row.

    """
    return {
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center",
        "padding": "0.5rem 1rem",
        "backgroundColor": "#34495e",
        "position": "relative",
        # Mobile adjustments
        "@media (max-width: 768px)": {
            "justifyContent": "space-between",
        },
    }


def logo_style() -> dict:
    """Define high-quality logo styles.

    Returns:
        dict: CSS styles for high-resolution logos.

    """
    return {
        "border": "2px solid #ecf0f1",
        "borderRadius": "6px",
        "padding": "4px",
        "backgroundColor": "#ffffff",
        "transition": "transform 0.2s ease",
        # High-quality rendering
        "imageRendering": "high-quality",
        "imageResolution": "from-image",
        "objectFit": "contain",
        "objectPosition": "center",
        "filter": "contrast(1.05) brightness(1.02)",
        # Anti-aliasing and smoothing
        "backfaceVisibility": "hidden",
        "perspective": "1000px",
        "transform": "translateZ(0)",
        "willChange": "transform",
        ":hover": {
            "transform": "scale(1.05) translateZ(0)",
            "filter": "contrast(1.08) brightness(1.04)",
        },
        # Mobile responsive sizing
        "@media (max-width: 768px)": {
            "border": "1px solid #ecf0f1",
            "padding": "3px",
        },
        "@media (max-width: 576px)": {
            "border": "1px solid #ecf0f1",
            "padding": "2px",
        },
    }


def brand_style() -> dict:
    """Define brand text styles.

    Returns:
        dict: CSS styles for brand text.

    """
    return {
        "fontSize": "2.75rem",
        "fontWeight": "700",
        "color": "#ecf0f1",
        "fontFamily": "Arial, sans-serif",
        "textShadow": "1px 1px 2px rgba(0, 0, 0, 0.3)",
        "textAlign": "center",
        "whiteSpace": "nowrap",
        "margin": "0",
        # Mobile responsive
        "@media (max-width: 768px)": {
            "fontSize": "1.5rem",
        },
        "@media (max-width: 576px)": {
            "fontSize": "1.25rem",
        },
        "@media (max-width: 480px)": {
            "fontSize": "1.1rem",
            "whiteSpace": "normal",
            "lineHeight": "1.2",
        },
    }


def brand_container_style() -> dict:
    """Define brand container styles.

    Returns:
        dict: CSS styles for brand container.

    """
    return {
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center",
        "gap": "2rem",
        "flexWrap": "wrap",
        # Mobile responsive
        "@media (max-width: 768px)": {
            "flexDirection": "column",
            "gap": "1rem",
        },
        "@media (max-width: 576px)": {
            "gap": "0.5rem",
        },
    }


def logos_container_style() -> dict:
    """Define logos container styles.

    Returns:
        dict: CSS styles for logos container.

    """
    return {
        "display": "flex",
        "alignItems": "center",
        "gap": "1.5rem",
        # Mobile responsive
        "@media (max-width: 768px)": {
            "gap": "1rem",
        },
        "@media (max-width: 576px)": {
            "gap": "0.75rem",
        },
    }


def nav_link_style() -> dict:
    """Define navigation link styles.

    Returns:
        dict: CSS styles for nav links.

    """
    return {
        "color": "#ecf0f1",
        "fontWeight": "500",
        "fontSize": "1.1rem",
        "padding": "0.75rem 1rem",
        "borderRadius": "6px",
        "transition": "all 0.3s ease",
        "margin": "0 0.25rem",
        "fontFamily": "Arial, sans-serif",
        "textDecoration": "none",
        ":hover": {
            "backgroundColor": "#3498db",
            "color": "#ffffff",
            "transform": "translateY(-1px)",
        },
        # Mobile styles
        "@media (max-width: 768px)": {
            "fontSize": "1rem",
            "padding": "0.5rem 0.75rem",
            "margin": "0.25rem 0",
            "width": "100%",
            "textAlign": "center",
        },
    }


def hamburger_style() -> dict:
    """Define hamburger menu styles.

    Returns:
        dict: CSS styles for hamburger menu.

    """
    return {
        "border": "2px solid #3498db",
        "borderRadius": "6px",
        "padding": "0.5rem",
        "backgroundColor": "transparent",
        "color": "#ecf0f1",
        "fontSize": "1.2rem",
        ":focus": {
            "boxShadow": "0 0 0 0.2rem rgba(52, 152, 219, 0.5)",
        },
        ":hover": {
            "backgroundColor": "#3498db",
        },
    }


def footer_style() -> dict:
    """Define footer styles.

    Returns:
        dict: CSS styles for footer.

    """
    return {
        "backgroundColor": "#2c3e50",
        "color": "#ecf0f1",
        "padding": "2rem 0",
        "marginTop": "3rem",
        "borderTop": "3px solid #3498db",
        "fontFamily": "Arial, sans-serif",
        "fontSize": "0.9rem",
        # Mobile responsive
        "@media (max-width: 768px)": {
            "padding": "1.5rem 0",
            "fontSize": "0.8rem",
        },
    }


def footer_section_style() -> dict:
    """Define footer section styles.

    Returns:
        dict: CSS styles for footer sections.

    """
    return {
        "marginBottom": "1.5rem",
        # Mobile responsive
        "@media (max-width: 768px)": {
            "marginBottom": "1rem",
            "textAlign": "center",
        },
    }


def footer_title_style() -> dict:
    """Define footer title styles.

    Returns:
        dict: CSS styles for footer titles.

    """
    return {
        "color": "#3498db",
        "fontSize": "1.1rem",
        "fontWeight": "600",
        "marginBottom": "0.75rem",
        "borderBottom": "1px solid #34495e",
        "paddingBottom": "0.5rem",
        # Mobile responsive
        "@media (max-width: 768px)": {
            "fontSize": "1rem",
            "marginBottom": "0.5rem",
        },
    }


def footer_link_style() -> dict:
    """Define footer link styles.

    Returns:
        dict: CSS styles for footer links.

    """
    return {
        "color": "#bdc3c7",
        "textDecoration": "none",
        "transition": "color 0.3s ease",
        "display": "block",
        "padding": "0.25rem 0",
        ":hover": {
            "color": "#3498db",
            "textDecoration": "underline",
        },
    }


def footer_copyright_style() -> dict:
    """Define footer copyright styles.

    Returns:
        dict: CSS styles for copyright section.

    """
    return {
        "borderTop": "1px solid #34495e",
        "paddingTop": "1.5rem",
        "marginTop": "2rem",
        "textAlign": "center",
        "color": "#95a5a6",
        "fontSize": "0.85rem",
        # Mobile responsive
        "@media (max-width: 768px)": {
            "paddingTop": "1rem",
            "marginTop": "1.5rem",
            "fontSize": "0.8rem",
        },
    }


# Footer component
footer = html.Footer(
    [
        dbc.Container(
            [
                dbc.Row(
                    [
                        # Organization Info
                        dbc.Col(
                            [
                                html.H5("ASSAS Project", style=footer_title_style()),
                                html.A(
                                    html.Img(
                                        src=encode_svg_image_hq("assas_logo_mod.svg"),
                                        height="48px",
                                        style={
                                            "backgroundColor": "#fff",
                                            "padding": "4px",
                                            "borderRadius": "4px",
                                            "margin": "0.5rem 0 1rem 0",
                                            "boxShadow": "0 2px 4px rgba(0,0,0,0.06)",
                                            "display": "block",
                                            "marginLeft": "auto",  # <-- add this
                                            "marginRight": "auto",  # <-- add this
                                        },
                                        alt="ASSAS Logo",
                                    ),
                                    href="https://assas-horizon-euratom.eu/",
                                    target="_blank",
                                    style={"display": "inline-block"},
                                ),
                                html.P(
                                    [
                                        html.B(
                                            "Artificial Intelligence for "
                                            "Simulation of Severe AccidentS (ASSAS)"
                                        ),
                                        html.Br(),
                                        "A European research initiative advancing "
                                        "nuclear safety through data-driven "
                                        "simulation and machine learning.",
                                        html.Br(),
                                    ],
                                    style={"lineHeight": "1.6", "marginBottom": "1rem"},
                                ),
                                html.Div(
                                    [
                                        html.A(
                                            [
                                                html.I(
                                                    className=("fas fa-globe me-2"),
                                                ),
                                                "ASSAS Project Website",
                                            ],
                                            href="https://assas-horizon-euratom.eu/",
                                            style=footer_link_style(),
                                            target="_blank",
                                        ),
                                    ]
                                ),
                                html.Div(
                                    [
                                        html.A(
                                            [
                                                html.I(
                                                    className="fas fa-envelope",
                                                    style={"marginRight": "0.4em"},
                                                ),
                                                "Contact",
                                            ],
                                            href="mailto:jonas.dressner@kit.edu",
                                            style=footer_link_style(),
                                        ),
                                    ],
                                    style={
                                        "display": "flex",
                                        "alignItems": "center",
                                        "justifyContent": "center",
                                    },
                                ),
                            ],
                            md=4,
                            sm=12,
                            style={**footer_section_style(), "textAlign": "center"},
                        ),
                        # Quick Links - Updated to use dynamic URLs
                        dbc.Col(
                            [
                                html.H5("Quick Links", style=footer_title_style()),
                                html.Div(
                                    id="footer-links",
                                    children=[
                                        html.A(
                                            "Home",
                                            href="#",
                                            style=footer_link_style(),
                                            id="footer-home-link",
                                        ),
                                        html.A(
                                            "Database",
                                            href="#",
                                            style=footer_link_style(),
                                            id="footer-database-link",
                                        ),
                                        html.A(
                                            "Profile",
                                            href="#",
                                            style=footer_link_style(),
                                            id="footer-profile-link",
                                        ),
                                        html.A(
                                            "Documentation",
                                            href="#",
                                            style=footer_link_style(),
                                            id="footer-documentation-link",
                                        ),
                                        html.A(
                                            "About",
                                            href="#",
                                            style=footer_link_style(),
                                            id="footer-about-link",
                                        ),
                                    ],
                                ),
                            ],
                            md=2,
                            sm=5,
                            style=footer_section_style(),
                        ),
                        # Resources
                        dbc.Col(
                            [
                                html.H5("Resources", style=footer_title_style()),
                                html.Div(
                                    [
                                        html.A(
                                            [
                                                html.I(
                                                    className=("fab fa-github me-2"),
                                                ),
                                                "GitHub ASSAS Database",
                                            ],
                                            href=(
                                                "https://github.com/"
                                                "Helmholtz-AI-Energy/"
                                                "assas_database"
                                            ),
                                            style=footer_link_style(),
                                            target="_blank",
                                        ),
                                        html.A(
                                            [
                                                html.I(
                                                    className=("fab fa-github me-2"),
                                                ),
                                                "GitHub ASSAS Data Hub",
                                            ],
                                            href=(
                                                "https://github.com/"
                                                "Helmholtz-AI-Energy/"
                                                "assas-data-hub"
                                            ),
                                            style=footer_link_style(),
                                            target="_blank",
                                        ),
                                        html.A(
                                            "Research Papers",
                                            href="#",
                                            style=footer_link_style(),
                                        ),
                                    ]
                                ),
                            ],
                            md=2,
                            sm=3,
                            style=footer_section_style(),
                        ),
                        # Partners & Social
                        dbc.Col(
                            [
                                html.H5("Partners", style=footer_title_style()),
                                html.Div(
                                    [
                                        # Partner logos
                                        html.Div(
                                            [
                                                html.A(
                                                    html.Img(
                                                        src=encode_svg_image_hq(
                                                            "kit_logo.drawio.svg"
                                                        ),
                                                        height="40px",
                                                        width="80px",
                                                        style={
                                                            "backgroundColor": (
                                                                "#ffffff"
                                                            ),
                                                            "padding": "4px",
                                                            "borderRadius": "4px",
                                                            "marginBottom": "1rem",
                                                            "filter": "contrast(1.05)"
                                                            "brightness(1.02)",
                                                        },
                                                        alt="KIT Logo",
                                                    ),
                                                    href="https://www.kit.edu/",
                                                    target="_blank",
                                                    style={
                                                        "display": "inline-block",
                                                        "marginRight": "1rem",
                                                    },
                                                ),
                                            ],
                                            style={"marginBottom": "1rem"},
                                        ),
                                        # Social links
                                        html.Div(
                                            [
                                                html.A(
                                                    [
                                                        html.I(
                                                            className=(
                                                                "fas fa-globe me-2"
                                                            ),
                                                        ),
                                                        "ASNR Website",
                                                    ],
                                                    href="https://research-assessment"
                                                    ".asnr.fr/about-us/",
                                                    style=footer_link_style(),
                                                    target="_blank",
                                                ),
                                                html.A(
                                                    [
                                                        html.I(
                                                            className=(
                                                                "fas fa-globe me-2"
                                                            ),
                                                        ),
                                                        "CIEMAT Website",
                                                    ],
                                                    href="https://www.ciemat.es/",
                                                    style=footer_link_style(),
                                                    target="_blank",
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ],
                            md=4,
                            sm=12,
                            style=footer_section_style(),
                        ),
                    ],
                    justify="center",
                    align="start",
                ),
                # Copyright section
                html.Hr(style={"borderColor": "#34495e", "margin": "2rem 0 1.5rem 0"}),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div(
                                    [
                                        html.P(
                                            [
                                                (
                                                    "© 2024 ASSAS Data Hub. "
                                                    "All rights reserved. | "
                                                ),
                                                html.A(
                                                    "Privacy Policy",
                                                    href="#",
                                                    style=footer_link_style(),
                                                    id="footer-privacy-link",
                                                ),
                                                " | ",
                                                html.A(
                                                    "Terms of Service",
                                                    href="#",
                                                    style=footer_link_style(),
                                                    id="footer-terms-link",
                                                ),
                                                " | ",
                                                html.A(
                                                    "Imprint",
                                                    href="#",
                                                    style=footer_link_style(),
                                                    id="footer-imprint-link",
                                                ),
                                            ],
                                            style={
                                                "margin": "0",
                                                "display": "flex",
                                                "alignItems": "center",
                                                "justifyContent": "center",
                                                "flexWrap": "wrap",
                                                "gap": "0.5rem",
                                            },
                                        ),
                                    ],
                                    style=footer_copyright_style(),
                                )
                            ],
                            width=12,
                        )
                    ]
                ),
            ],
            fluid=True,
            style={"textAlign": "center"},
        )
    ],
    style=footer_style(),
)


def create_navbar_header() -> html.Div:
    """Create the navbar header."""
    header = (
        html.Div(
            [
                dbc.Container(
                    [
                        html.Div(
                            [
                                # Left logo
                                html.Div(
                                    html.A(
                                        html.Img(
                                            src=encode_svg_image_hq(
                                                "assas_logo_mod.svg"
                                            ),
                                            height="80px",
                                            width="160px",
                                            style=logo_style(),
                                            alt="ASSAS Logo",
                                            className="logo-high-quality logo-assas",
                                        ),
                                        href="https://assas-horizon-euratom.eu/",
                                        target="_blank",
                                        style={"display": "inline-block"},
                                    ),
                                    style={"flex": "0 0 180px", "textAlign": "center"},
                                ),
                                # Title
                                html.Div(
                                    html.A(
                                        html.H1(
                                            "ASSAS Data Hub",
                                            style=brand_style(),
                                            className="brand-title brand-center",
                                        ),
                                        href="#",
                                        style={"textDecoration": "none"},
                                        className="brand-link",
                                        id="navbar-title-link",
                                    ),
                                    style={"flex": "1 1 auto", "textAlign": "center"},
                                ),
                                # Right logo
                                html.Div(
                                    html.A(
                                        html.Img(
                                            src=encode_svg_image_hq(
                                                "kit_logo.drawio.svg"
                                            ),
                                            height="80px",
                                            width="160px",
                                            style=logo_style(),
                                            alt="KIT Logo",
                                            className="logo-high-quality logo-kit",
                                        ),
                                        href="https://www.kit.edu/",
                                        target="_blank",
                                        style={"display": "inline-block"},
                                    ),
                                    style={"flex": "0 0 180px", "textAlign": "center"},
                                ),
                            ],
                            style=brand_container_style(),
                        )
                    ],
                    fluid=True,
                    style={"maxWidth": "1400px"},
                )
            ],
            style=top_row_style(),
        ),
    )

    return header


def navitem(show: bool, *args: object, **kwargs: object) -> dbc.NavItem:
    """Create a navbar item conditionally."""
    return dbc.NavItem(
        dbc.NavLink(*args, **kwargs), style={} if show else {"display": "none"}
    )


def create_navbar_items_role_based(allowed_pages: list[str]) -> list[dbc.NavItem]:
    """Create navbar items based on user role."""
    nav_items = []

    nav_items.append(
        navitem(
            "home" in allowed_pages or "visitor" in allowed_pages,
            [html.I(className="fas fa-home me-2"), "Home"],
            href="#",
            active="exact",
            style=nav_link_style(),
            className="nav-link-modern",
            id="nav-home-link",
        )
    )
    nav_items.append(
        navitem(
            "database" in allowed_pages,
            [html.I(className="fas fa-database me-2"), "Database"],
            href="#",
            active="exact",
            style=nav_link_style(),
            className="nav-link-modern",
            id="nav-database-link",
        )
    )
    nav_items.append(
        navitem(
            "profile" in allowed_pages,
            [html.I(className="fas fa-user me-2"), "Profile"],
            href="#",
            active="exact",
            style=nav_link_style(),
            className="nav-link-modern",
            id="nav-profile-link",
        )
    )
    nav_items.append(
        navitem(
            "upload" in allowed_pages,
            [html.I(className="fas fa-upload me-2"), "Upload"],
            href="#",
            active="exact",
            style=nav_link_style(),
            className="nav-link-modern",
            id="nav-upload-link",
        )
    )
    nav_items.append(
        navitem(
            "admin" in allowed_pages,
            [html.I(className="fas fa-users-cog me-2"), "Admin"],
            href="#",
            active="exact",
            style=nav_link_style(),
            className="nav-link-modern",
            id="nav-admin-link",
        )
    )
    nav_items.append(
        navitem(
            "about" in allowed_pages,
            [html.I(className="fas fa-info-circle me-2"), "About"],
            href="#",
            active="exact",
            style=nav_link_style(),
            className="nav-link-modern",
            id="nav-about-link",
        )
    )
    nav_items.append(
        navitem(
            "documentation" in allowed_pages,
            [html.I(className="fas fa-book me-2"), "Documentation"],
            href="#",
            active="exact",
            style=nav_link_style(),
            className="nav-link-modern",
            id="nav-documentation-link",
        )
    )

    logger.info(f"Navbar items for role: {allowed_pages}, {len(nav_items)}")
    return nav_items


def serve_layout() -> html.Div:
    """Serve the main layout of the application."""
    role = get_user_role()
    allowed_pages = get_allowed_pages(role)
    logger.info(f"User role: {role}, Allowed pages: {allowed_pages}.")
    navbar = create_navbar(allowed_pages)
    return html.Div(
        [
            dcc.Location(id="url", refresh=False),
            navbar,
            html.Div(
                [
                    html.Div(
                        dash.page_container,
                        className="responsive-content",
                        style={
                            "minHeight": "calc(100vh - 160px - 200px)",
                            "paddingTop": "160px",
                            "paddingBottom": "2rem",
                            # "paddingLeft": "1vw",
                            # "paddingRight": "1vw",
                            # "marginLeft": "auto",
                            # "marginRight": "auto",
                            # "maxWidth": "1200px",
                            # "width": "100%",
                        },
                    ),
                    footer,
                ],
                style={
                    "display": "flex",
                    "flexDirection": "column",
                    "minHeight": "100vh",
                },
            ),
        ],
        id="dash-container",
        style={
            "fontFamily": "Arial, sans-serif",
            "backgroundColor": "#f8f9fa",
        },
    )


def create_navbar(allowed_pages: list[str]) -> html.Div:
    """Create navbar with dynamic URLs."""
    return html.Div(
        [
            *create_navbar_header(),
            html.Div(
                [
                    dbc.Container(
                        [
                            html.Div(
                                [
                                    dbc.Button(
                                        html.I(className="fas fa-bars"),
                                        id="navbar-toggler",
                                        n_clicks=0,
                                        style=hamburger_style(),
                                        className="navbar-toggler-mobile d-md-none",
                                    ),
                                    dbc.Collapse(
                                        [
                                            dbc.Nav(
                                                [
                                                    *create_navbar_items_role_based(
                                                        allowed_pages
                                                    )
                                                ],
                                                className="nav-items-container",
                                                horizontal=True,
                                                style={
                                                    "display": "flex",
                                                    "alignItems": "center",
                                                    "justifyContent": "center",
                                                    "gap": "1rem",
                                                    "width": "100%",
                                                },
                                            )
                                        ],
                                        id="navbar-collapse",
                                        is_open=False,
                                        className="navbar-collapse-custom",
                                    ),
                                ],
                                style={
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                    "width": "100%",
                                    "position": "relative",
                                },
                            )
                        ],
                        fluid=True,
                    )
                ],
                style=bottom_row_style(),
            ),
            html.Div(id="scroll-progress", className="scroll-indicator"),
        ],
        id="main-navbar",
        style=modern_navbar_style(),
        className="navbar-two-row sticky-top",
    )


def init_dashboard(server: object) -> object:
    """Create a Plotly Dash dashboard."""
    pages_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pages")
    assets_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
    logger.debug("pages folder %s, assets_folder %s" % (pages_folder, assets_folder))

    launch_uid = uuid.uuid4()

    cache = diskcache.Cache("./cache")
    long_callback_manager = DiskcacheLongCallbackManager(
        cache,
        cache_by=[lambda: launch_uid],
        expire=60,
    )

    init_oauth(server)

    # Protect Dash pages
    @server.before_request
    def restrict_access() -> Response:
        """Restrict access to Dash pages for unauthenticated users."""
        path = request.path
        logger.info(f"Checking authentication for path: {path}")

        # Allow static and Dash internal routes
        if (
            path.startswith("/static/")
            or path.startswith("/assets/")
            or path.startswith("/_dash-")
            or path.startswith("/_dash/")
            or path.startswith("/_dash-component-suites/")
            or path.endswith(".js")
            or path.endswith(".css")
            or path.endswith(".map")
            or path.endswith(".ico")
            or path.endswith(".png")
            or path.endswith(".svg")
            or path.endswith(".jpg")
            or path.endswith(".woff2")
            or path.endswith(".ttf")
            or path.endswith("favicon.ico")
            or path.endswith("robots.txt")
            or path.endswith("/_dash-layout")
            or path.endswith("/_dash-dependencies")
            or path.endswith("/_dash-update-component")
            or path.endswith("/terms")
            or path.endswith("/privacy")
        ):
            logger.info(f"Allowing static, Dash internal, or public route: {path}.")
            return None

        # Allow auth routes
        auth_base = get_auth_base_url()
        if path.startswith(f"{auth_base}/"):
            logger.info("Allowing auth route")
            return None

        # for development
        if path.startswith(f"{get_base_url()}/companion/"):
            logger.info("Allowing development route")
            return None

        # Check authentication for Dash app routes
        dash_base = get_base_url()
        if path.startswith(f"{dash_base}/"):
            current_user = get_current_user()
            user_role = get_user_role()
            logger.info(
                f"Current user for Dash access: {current_user} with role: {user_role}"
            )

            if not is_authenticated():
                logger.info("User not authenticated, redirecting to login.")
                session["next_url"] = request.url
                return redirect(build_auth_url("/login"))

            if user_role == "visitor":
                if not (
                    request.path.endswith("/visitor")
                    or request.path.endswith("/profile")
                ):
                    visitor_url = build_url("/visitor", get_base_url())
                    logger.info(f"Redirecting from {path} to: {visitor_url}")
                    return redirect(visitor_url)

            logger.info(
                f"User {current_user.get('email')} authenticated, allowing access."
            )

        return None

    dash_app = dash.Dash(
        server=server,
        url_base_pathname=get_dash_base_url(),  # Use dynamic base URL
        title="ASSAS Data Hub",
        external_stylesheets=[
            dbc.themes.BOOTSTRAP,
            "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css",
        ],
        use_pages=True,
        pages_folder=pages_folder,
        assets_folder=assets_folder,
        long_callback_manager=long_callback_manager,
        suppress_callback_exceptions=True,
    )

    logger.info(f"Dash app initialized with pages: {ds.page_registry.keys()}.")

    # Add callback to update navbar and footer links dynamically
    @dash_app.callback(
        [
            Output("navbar-title-link", "href"),
            Output("nav-home-link", "href"),
            Output("nav-database-link", "href"),
            Output("nav-profile-link", "href"),
            Output("nav-upload-link", "href"),
            Output("nav-admin-link", "href"),
            Output("nav-documentation-link", "href"),
            Output("nav-about-link", "href"),
            Output("footer-home-link", "href"),
            Output("footer-database-link", "href"),
            Output("footer-profile-link", "href"),
            Output("footer-documentation-link", "href"),
            Output("footer-about-link", "href"),
            Output("footer-privacy-link", "href"),
            Output("footer-terms-link", "href"),
            Output("footer-imprint-link", "href"),
        ],
        Input("url", "pathname"),
        prevent_initial_call=False,
    )
    def update_navigation_links(pathname: str) -> tuple[str, ...]:
        """Update all navigation links to use current base URL."""
        base_url = get_base_url()
        role = get_user_role()
        logger.info(f"Current pathname: {pathname}.")
        logger.info(
            f"Updating navigation links for role: {role}, base_url: {base_url}."
        )
        as_visitor = role == "visitor"

        result = tuple(
            str(x)
            for x in [
                f"{base_url}/visitor" if as_visitor else f"{base_url}/home",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/home",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/database",
                f"{base_url}/profile",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/upload",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/admin",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/documentation",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/about",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/home",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/database",
                f"{base_url}/profile",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/documentation",
                f"{base_url}/visitor" if as_visitor else f"{base_url}/about",
                f"{base_url}/privacy",
                f"{base_url}/terms",
                f"{base_url}/imprint",
            ]
        )
        logger.info(f"Navigation links: {result}")
        return result

    # Navbar toggle callback - WORKING BURGER MENU
    @dash_app.callback(
        Output("navbar-collapse", "is_open"),
        [Input("navbar-toggler", "n_clicks")],
        [State("navbar-collapse", "is_open")],
        prevent_initial_call=True,
    )
    def toggle_navbar_collapse(
        n_clicks: int | None,
        is_open: bool,
    ) -> bool:
        """Toggle the navbar collapse state.

        Args:
            n_clicks (int | None): Number of clicks on the toggle button.
            is_open (bool): Current state of the navbar (open or closed).

        Returns:
            bool: New state of the navbar (open or closed).

        """
        if n_clicks:
            return not is_open
        return is_open

    dash_app.clientside_callback(
        """
function(id) {
    // Cache DOM references once (fixes "navbar is not defined")
    const navbar = document.getElementById('main-navbar');
    const scrollIndicator = document.getElementById('scroll-progress');

    if (!navbar) {
        return {};
    }

    let lastScrollTop = 0;

    function handleScroll() {
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        const scrollHeight = document.documentElement.scrollHeight - window.innerHeight;
        const scrollProgress = scrollTop / scrollHeight;

        const navbar = document.getElementById('main-navbar');
        const scrollIndicator = document.getElementById('scroll-progress');

        if (!navbar) return {};

        // Update scroll indicator
        if (scrollIndicator) {
            scrollIndicator.style.transform = `scaleX(${scrollProgress})`;
        }

        // Clear existing classes
        navbar.classList.remove('navbar-hidden', 'navbar-visible');

        if (scrollTop <= 5) {
            // At top of page (within 5px) - show navbar
            navbar.classList.add('navbar-visible');
            navbar.style.transform = 'translateY(0)';
            navbar.style.opacity = '1';
            navbar.style.transition = 'all 0.3s ease';
        } else {
            // Any scroll away from top - hide navbar immediately
            navbar.classList.add('navbar-hidden');
            navbar.style.transform = 'translateY(-100%)';
            navbar.style.opacity = '0';
            navbar.style.transition = 'all 0.2s ease';  // Faster hiding
        }

        lastScrollTop = scrollTop;

        return {};
    }

    // Throttled scroll handler for better performance
    let ticking = false;

    function requestTick() {
        if (!ticking) {
            requestAnimationFrame(handleScroll);
            ticking = true;
            setTimeout(() => { ticking = false; }, 8); // ~120fps for faster response
        }
    }

    // Add scroll listener
    window.addEventListener('scroll', requestTick, { passive: true });

    // Remove mouse movement show behavior - only show at top

    // Handle window resize
    window.addEventListener('resize', function() {
        if (navbar) {
            const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
            if (scrollTop <= 5) {
                navbar.classList.remove('navbar-hidden');
                navbar.classList.add('navbar-visible');
                navbar.style.transform = 'translateY(0)';
                navbar.style.opacity = '1';
            }
        }
    });

    return {};
}
""",
        Output("scroll-progress", "style"),
        Input("main-navbar", "id"),
        prevent_initial_call=False,
    )

    dash_app.layout = serve_layout
    server.dash_app = dash_app

    return server
