"""Routes for parent Flask app."""

import logging
import uuid
import shutil
import pandas as pd

from flask import current_app as app
from pathlib import Path
from flask import (
    session,
    request,
    redirect,
    send_file,
    jsonify,
    Response,
    render_template,
)

from flask_app import get_mongo_client
from .auth_utils import (
    auth,
    users,
)
from .utils.url_utils import get_base_url, build_url
from assasdb import AssasDatabaseManager, AssasDatabaseHandler

logger = logging.getLogger("assas_app")


@app.route("/")
def from_root_to_home() -> Response:
    """Redirect from the root URL to the home page."""
    logger.info("Redirecting from root to home page.")
    return redirect(build_url("/home"))


base_url = get_base_url()


@app.route(f"{base_url}/terms")
def terms() -> Response:
    """Render the terms of service page."""
    return render_template("terms.html")


@app.route(f"{base_url}/privacy")
def privacy() -> Response:
    """Render the privacy policy page."""
    return render_template("privacy.html")


# Legacy auth routes (consider moving to API)
@app.route("/login")
@auth.login_required
def check_user() -> Response:
    """Legacy route - consider using /api/v1/auth/user instead."""
    current_user = auth.current_user()
    user_info = users.get(current_user)

    if user_info:
        logger.info(f"User {current_user} authenticated via legacy route.")
        return jsonify(
            message=f"Hello, {current_user}!",
            email=user_info["email"],
            institute=user_info["institute"],
            role=user_info["role"],
        )
    else:
        return jsonify(error="User information not found"), 404


@app.route("/logout", methods=["GET"])
@auth.login_required
def logout() -> Response:
    """Legacy logout - consider using POST /api/v1/auth/logout instead."""
    current_user = auth.current_user()
    logger.info(f"Legacy logout for user: {current_user}")

    session.clear()
    response = jsonify(message="You have been logged out.")
    response.status_code = 401
    return response


def register_legacy_routes() -> None:
    """Register legacy routes that redirect to new API."""
    base_url = get_base_url()

    @app.route(f"{base_url}")
    @app.route(f"{base_url}/")
    def redirect_to_home() -> Response:
        """Redirect base URL to home."""
        return redirect(build_url("/home"))


def register_dynamic_routes() -> None:
    """Register routes dynamically based on BASE_URL configuration."""
    base_url = get_base_url()

    @app.route(f"{base_url}/hdf5_file", methods=["GET"])
    @auth.login_required
    def get_data_file() -> Response:
        """Handle the request to retrieve a data file based on the provided UUID."""
        logger.info("Received request for data file.")

        args = request.args
        system_uuid = uuid.UUID(args.get("uuid", type=str))

        manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )
        document = manager.get_database_entry_by_uuid(system_uuid)

        if document is None:
            logger.error(f"No document found for UUID {system_uuid}.")
            return jsonify(error="Document not found"), 404

        logger.info(f"Retrieving data file for UUID {system_uuid}.")
        filepath = document["system_result"]
        file = Path(filepath)

        if file.exists():
            logger.info(f"Handle request of {str(file)}.")

            return send_file(
                path_or_file=filepath,
                download_name=f"dataset_{system_uuid}.h5",
                as_attachment=True,
            )

    @app.route(f"{base_url}/hdf5_download", methods=["GET"])
    @auth.login_required
    def get_download_archive() -> Response:
        """Handle the request to download an archive based on the provided UUID."""
        logger.info("Received request for download archive.")

        args = request.args
        system_uuid = args.get("uuid", type=str)

        tmp_folder = app.config["TMP_FOLDER"]
        tmp_download_folder = f"{tmp_folder}/download_{system_uuid}"

        filepath = f"{tmp_download_folder}/download_{system_uuid}.zip"
        file = Path(filepath)

        if file.exists():
            logger.info(f"Handle request of {str(file)}.")

            response = send_file(
                path_or_file=filepath,
                download_name=f"download_{system_uuid}.zip",
                as_attachment=True,
            )

            try:
                shutil.rmtree(tmp_download_folder)
                logger.info(f"Temporary folder {tmp_download_folder} has been deleted.")

            except Exception as e:
                logger.error(f"Error deleting file {filepath}: {e}")

            return response

        else:
            logger.error(f"File not found: {filepath}.")
            return jsonify(error="Document not found"), 404

    @app.route(f"{base_url}/query_meta_data", methods=["GET"])
    @auth.login_required
    def query_data() -> Response:
        """Handle the request to retrieve meta data variables based on provided UUID."""
        args = request.args
        logger.info(f"Received request with arguments: {args}")

        system_uuid = uuid.UUID(args.get("uuid", type=str))

        manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )
        document = manager.get_database_entry_by_uuid(system_uuid)
        if document is None:
            logger.error(f"No document found for UUID {system_uuid}.")
            return jsonify(error="Document not found"), 404

        logger.info(f"Retrieving meta data for UUID {system_uuid}.")
        filepath = document["system_result"]
        meta_data_variables = document["meta_data_variables"]
        logger.info(f"Filepath: {filepath}")
        logger.info(f"Meta data variables: {meta_data_variables}")

        return jsonify(
            {
                "meta_data_variables": meta_data_variables,
            }
        )

    @app.route(f"{base_url}/datasets", methods=["GET"])
    @auth.login_required
    def get_datasets() -> Response:
        """Get all datasets or filter by name, return as JSON."""
        logger.info("Received request for datasets.")

        # Get query parameters
        name_filter = request.args.get("name", None)
        format_type = request.args.get("format", "json").lower()  # json or dataframe

        manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )

        try:
            # Get all documents
            documents = manager.database_handler.get_all_file_documents()

            if not documents:
                logger.info("No datasets found.")
                return jsonify({"datasets": [], "count": 0})

            # Convert to list of dicts
            datasets = []
            for doc in documents:
                dataset = {
                    "uuid": str(doc.get("system_uuid")),
                    "name": doc.get("meta_name", ""),
                    "status": doc.get("system_status", ""),
                    "date": doc.get("system_date", ""),
                    "user": doc.get("system_user", ""),
                    "size": doc.get("system_size", ""),
                    "size_hdf5": doc.get("system_size_hdf5", ""),
                    "description": doc.get("meta_description", ""),
                    "keywords": doc.get("meta_keywords", []),
                    "tags": doc.get("meta_tags", []),
                }
                datasets.append(dataset)

            # Filter by name if provided
            if name_filter:
                datasets = [
                    d for d in datasets if name_filter.lower() in d["name"].lower()
                ]
                logger.info(
                    f"Filtered datasets by name '{name_filter}': {len(datasets)} found."
                )

            # Return based on format
            if format_type == "dataframe":
                # Convert to DataFrame and return as CSV-like JSON
                df = pd.DataFrame(datasets)
                return jsonify(
                    {
                        "format": "dataframe",
                        "columns": df.columns.tolist(),
                        "data": df.to_dict("records"),
                        "count": len(datasets),
                    }
                )
            else:
                # Return as regular JSON
                return jsonify(
                    {
                        "datasets": datasets,
                        "count": len(datasets),
                        "filter": name_filter if name_filter else "none",
                    }
                )

        except Exception as e:
            logger.error(f"Error retrieving datasets: {e}")
            return jsonify(error="Failed to retrieve datasets"), 500

    @app.route(f"{base_url}/datasets/search", methods=["GET"])
    @auth.login_required
    def search_datasets() -> Response:
        """Advanced search for datasets with multiple filters."""
        logger.info("Received request for dataset search.")

        # Get query parameters
        name = request.args.get("name")
        status = (
            request.args.get("status").capitalize()
            if request.args.get("status")
            else None
        )
        user = request.args.get("user")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        format_type = request.args.get("format", "json").lower()

        manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )

        try:
            # Build query filters
            query_filters = {}

            if name:
                query_filters["meta_name"] = {
                    "$regex": name,
                    "$options": "i",
                }  # Case-insensitive
            if status:
                query_filters["system_status"] = status
            if user:
                query_filters["system_user"] = user
            if date_from or date_to:
                date_filter = {}
                if date_from:
                    date_filter["$gte"] = date_from
                if date_to:
                    date_filter["$lte"] = date_to
                query_filters["system_date"] = date_filter

            # Get filtered documents
            if query_filters:
                documents = manager.database_handler.file_collection.find(query_filters)
            else:
                documents = manager.database_handler.get_all_file_documents()

            # Convert to list
            datasets = []
            for doc in documents:
                dataset = {
                    "uuid": str(doc.get("system_uuid")),
                    "name": doc.get("meta_name", ""),
                    "status": doc.get("system_status", ""),
                    "date": doc.get("system_date", ""),
                    "user": doc.get("system_user", ""),
                    "size": doc.get("system_size", ""),
                    "size_hdf5": doc.get("system_size_hdf5", ""),
                    "description": doc.get("meta_description", ""),
                }
                datasets.append(dataset)

            logger.info(f"Search found {len(datasets)} datasets.")

            if format_type == "dataframe":
                df = pd.DataFrame(datasets)
                return jsonify(
                    {
                        "format": "dataframe",
                        "columns": df.columns.tolist(),
                        "data": df.to_dict("records"),
                        "count": len(datasets),
                        "filters": query_filters,
                    }
                )
            else:
                return jsonify(
                    {
                        "datasets": datasets,
                        "count": len(datasets),
                        "filters": query_filters,
                    }
                )

        except Exception as e:
            logger.error(f"Error searching datasets: {e}")
            return jsonify(error="Failed to search datasets"), 500

    @app.route(f"{base_url}/datasets/<uuid:dataset_id>", methods=["GET"])
    @auth.login_required
    def get_dataset_by_id(dataset_id: str) -> Response:
        """Get a specific dataset by UUID."""
        logger.info(f"Received request for dataset {dataset_id}.")

        manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                client=get_mongo_client(app.config["CONNECTIONSTRING"]),
                backup_directory=app.config["BACKUP_DIRECTORY"],
                database_name=app.config["MONGO_DB_NAME"],
            )
        )

        try:
            dataset_id = uuid.UUID(dataset_id)
            document = manager.get_database_entry_by_uuid(dataset_id)

            if not document:
                return jsonify(error="Dataset not found"), 404

            dataset = {
                "uuid": str(document.get("system_uuid")),
                "name": document.get("meta_name", ""),
                "status": document.get("system_status", ""),
                "date": document.get("system_date", ""),
                "user": document.get("system_user", ""),
                "size": document.get("system_size", ""),
                "size_hdf5": document.get("system_size_hdf5", ""),
                "description": document.get("meta_description", ""),
                "keywords": document.get("meta_keywords", []),
                "tags": document.get("meta_tags", []),
                "meta_data_variables": document.get("meta_data_variables", {}),
                "filepath": document.get("system_path", ""),
                "result_filepath": document.get("system_result", ""),
            }

            return jsonify(dataset)

        except Exception as e:
            logger.error(f"Error retrieving dataset {dataset_id}: {e}")
            return jsonify(error="Failed to retrieve dataset"), 500


def init_routes() -> None:
    """Initialize routes - now mainly legacy support."""
    register_legacy_routes()
    # register_dynamic_routes()
    logger.info("Legacy routes initialized. Consider migrating to /api/v1/ endpoints.")
