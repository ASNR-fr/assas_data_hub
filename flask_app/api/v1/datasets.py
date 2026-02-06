"""RESTful Dataset API endpoints."""

import logging
import uuid
import re  # <-- add
import numpy as np
import netCDF4 as nc
from datetime import datetime, timezone
from typing import Dict, Any, List, Union

from flask import Blueprint, request, Response
from flask import current_app as app

from ...auth_utils import auth, get_current_user
from ...utils.api_utils import APIResponse, handle_api_error
from assasdb import (
    AssasDatabaseManager,
    AssasDatabaseHandler,
    AssasNetCDF4VariableHandler,
)

logger = logging.getLogger("assas_app")

datasets_bp = Blueprint("datasets", __name__)


class DatasetService:
    """Service class for dataset operations."""

    @staticmethod
    def get_database_manager() -> AssasDatabaseManager:
        """Get database manager instance."""
        return AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                database_name=app.config["MONGO_DB_NAME"],
            )
        )

    @staticmethod
    def get_variable_handler() -> AssasNetCDF4VariableHandler:
        """Get NetCDF4 variable handler instance."""
        # Just create a handler without database handler for direct file access
        return AssasNetCDF4VariableHandler()

    @staticmethod
    def serialize_dataset(
        doc: Dict[str, Any], include_full_data: bool = False
    ) -> Dict[str, Any]:
        """Serialize dataset document for API response."""
        dataset = {
            "uuid": str(doc.get("system_uuid")),
            "upload_uuid": str(doc.get("system_upload_uuid")),
            "name": doc.get("meta_name", ""),
            "description": doc.get("meta_description", ""),
            "status": doc.get("system_status", ""),
            "created_date": doc.get("system_date", ""),
            "created_by": doc.get("system_user", ""),
            "size": doc.get("system_size", ""),
            "size_hdf5": doc.get("system_size_hdf5", ""),
        }

        if include_full_data:
            dataset.update(
                {
                    "meta_data_variables": doc.get("meta_data_variables", {}),
                    "filepath": doc.get("system_path", ""),
                    "result_filepath": doc.get("system_result", ""),
                }
            )

        return dataset

    @staticmethod
    def serialize_variable_info(var_info: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize variable information for API response."""
        return {
            "name": var_info.get("name", ""),
            "long_name": var_info.get("long_name", ""),
            "unit": var_info.get("unit", ""),
            "dimensions": var_info.get("dimensions", ""),
            "shape": var_info.get("shape", ""),
            "domain": var_info.get("domain", ""),
            "strategy": var_info.get("strategy", ""),
            "group": var_info.get("group", ""),
            "subgroup": var_info.get("subgroup", ""),
        }

    @staticmethod
    def serialize_numpy_array(data: np.ndarray) -> Union[List, float, int]:
        """Convert numpy array to JSON-serializable format."""
        if data.ndim == 0:
            # Scalar value
            return float(data) if np.isfinite(data) else None
        elif data.ndim == 1:
            # 1D array
            return [float(x) if np.isfinite(x) else None for x in data]
        else:
            # Multi-dimensional array - convert to nested lists
            return data.tolist()

    @staticmethod
    def serialize_statistics(stats: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize statistics data for JSON response."""
        if not stats:
            return {}

        serialized = {}
        for key, value in stats.items():
            if value is None:
                serialized[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                # Convert numpy types to Python types
                try:
                    float_val = float(value)
                    serialized[key] = float_val if np.isfinite(float_val) else None
                except (ValueError, OverflowError):
                    serialized[key] = None
            elif isinstance(value, np.ndarray):
                serialized[key] = DatasetService.serialize_numpy_array(value)
            elif isinstance(value, (list, tuple)):
                # Handle lists/tuples that might contain numpy types
                serialized[key] = [
                    float(item)
                    if isinstance(item, (np.integer, np.floating)) and np.isfinite(item)
                    else item
                    for item in value
                ]
            elif isinstance(value, dict):
                serialized[key] = DatasetService.serialize_statistics(
                    value
                )  # Recursive
            else:
                serialized[key] = value

        return serialized

    @staticmethod
    def serialize_group_info(group_info: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize group information for API response.

        Args:
            group_info: Group information dictionary from NetCDF4 handler.

        Returns:
            Dict[str, Any]: Serialized group information.

        """
        if not group_info:
            return {}

        def serialize_group_recursive(group_data: Dict[str, Any]) -> Dict[str, Any]:
            """Recursively serialize group data."""
            serialized = {
                "path": group_data.get("path", ""),
                "attributes": {},
                "dimensions": {},
                "variables": {},
                "subgroups": {},
            }

            # Serialize attributes
            attributes = group_data.get("attributes", {})
            for attr_name, attr_value in attributes.items():
                if isinstance(attr_value, (np.integer, np.floating)):
                    serialized["attributes"][attr_name] = (
                        float(attr_value) if np.isfinite(attr_value) else None
                    )
                elif isinstance(attr_value, np.ndarray):
                    serialized["attributes"][attr_name] = (
                        DatasetService.serialize_numpy_array(attr_value)
                    )
                else:
                    serialized["attributes"][attr_name] = (
                        str(attr_value) if attr_value is not None else ""
                    )

            # Serialize dimensions
            dimensions = group_data.get("dimensions", {})
            for dim_name, dim_info in dimensions.items():
                serialized["dimensions"][dim_name] = {
                    "size": int(dim_info.get("size", 0)),
                    "unlimited": bool(dim_info.get("unlimited", False)),
                }

            # Serialize variables metadata (not the actual data)
            variables = group_data.get("variables", {})
            for var_name, var_info in variables.items():
                serialized_var = {
                    "name": var_name,
                    "dimensions": list(var_info.get("dimensions", [])),
                    "shape": list(var_info.get("shape", [])),
                    "dtype": str(var_info.get("dtype", "")),
                    "size": (
                        int(var_info.get("size", 0)) if "size" in var_info else None
                    ),
                    "attributes": {},
                }

                # Serialize variable attributes
                var_attributes = var_info.get("attributes", {})
                for attr_name, attr_value in var_attributes.items():
                    if isinstance(attr_value, (np.integer, np.floating)):
                        serialized_var["attributes"][attr_name] = (
                            float(attr_value) if np.isfinite(attr_value) else None
                        )
                    elif isinstance(attr_value, np.ndarray):
                        serialized_var["attributes"][attr_name] = (
                            DatasetService.serialize_numpy_array(attr_value)
                        )
                    else:
                        serialized_var["attributes"][attr_name] = (
                            str(attr_value) if attr_value is not None else ""
                        )

                serialized["variables"][var_name] = serialized_var

            # Recursively serialize subgroups
            subgroups = group_data.get("subgroups", {})
            for subgroup_name, subgroup_info in subgroups.items():
                serialized["subgroups"][subgroup_name] = serialize_group_recursive(
                    subgroup_info
                )

            # Add summary statistics
            serialized["summary"] = {
                "total_variables": len(serialized["variables"]),
                "total_dimensions": len(serialized["dimensions"]),
                "total_subgroups": len(serialized["subgroups"]),
                "has_unlimited_dimensions": any(
                    dim.get("unlimited", False)
                    for dim in serialized["dimensions"].values()
                ),
            }

            return serialized

        return serialize_group_recursive(group_info)

    @staticmethod
    def serialize_group_structure_flat(group_info: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize group structure in a flat format for easier navigation.

        Args:
            group_info: Group information dictionary from NetCDF4 handler.

        Returns:
            Dict[str, Any]: Flattened group structure.

        """
        if not group_info:
            return {"groups": {}, "all_variables": {}, "all_dimensions": {}}

        def flatten_groups(group_data: Dict[str, Any], path: str = "") -> tuple:
            """Recursively flatten group structure."""
            groups = {}
            variables = {}
            dimensions = {}

            # Current group info
            current_path = path if path else "root"
            groups[current_path] = {
                "path": group_data.get("path", current_path),
                "variable_count": len(group_data.get("variables", {})),
                "dimension_count": len(group_data.get("dimensions", {})),
                "subgroup_count": len(group_data.get("subgroups", {})),
                "attributes": {
                    attr_name: str(attr_value) if attr_value is not None else ""
                    for attr_name, attr_value in group_data.get(
                        "attributes", {}
                    ).items()
                },
            }

            # Add variables with full paths
            for var_name, var_info in group_data.get("variables", {}).items():
                full_var_path = f"{path}/{var_name}" if path else var_name
                variables[full_var_path] = {
                    "name": var_name,
                    "group": current_path,
                    "dimensions": list(var_info.get("dimensions", [])),
                    "shape": list(var_info.get("shape", [])),
                    "dtype": str(var_info.get("dtype", "")),
                    "attributes": {
                        attr_name: str(attr_value) if attr_value is not None else ""
                        for attr_name, attr_value in var_info.get(
                            "attributes", {}
                        ).items()
                    },
                }

            # Add dimensions with full paths
            for dim_name, dim_info in group_data.get("dimensions", {}).items():
                full_dim_path = f"{path}/{dim_name}" if path else dim_name
                dimensions[full_dim_path] = {
                    "name": dim_name,
                    "group": current_path,
                    "size": int(dim_info.get("size", 0)),
                    "unlimited": bool(dim_info.get("unlimited", False)),
                }

            # Recursively process subgroups
            for subgroup_name, subgroup_info in group_data.get("subgroups", {}).items():
                subgroup_path = f"{path}/{subgroup_name}" if path else subgroup_name
                sub_groups, sub_variables, sub_dimensions = flatten_groups(
                    subgroup_info, subgroup_path
                )

                groups.update(sub_groups)
                variables.update(sub_variables)
                dimensions.update(sub_dimensions)

            return groups, variables, dimensions

        groups, variables, dimensions = flatten_groups(group_info)

        return {
            "groups": groups,
            "all_variables": variables,
            "all_dimensions": dimensions,
            "summary": {
                "total_groups": len(groups),
                "total_variables": len(variables),
                "total_dimensions": len(dimensions),
                "group_hierarchy": list(groups.keys()),
            },
        }

    @staticmethod
    def serialize_group_summary(group_info: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize a summary view of group structure.

        Args:
            group_info: Group information dictionary from NetCDF4 handler.

        Returns:
            Dict[str, Any]: Summary of group structure.

        """
        if not group_info:
            return {}

        def analyze_group(group_data: Dict[str, Any], path: str = "") -> Dict[str, Any]:
            """Analyze group structure for summary."""
            current_path = path if path else "root"

            # Count variables by type/domain
            variables = group_data.get("variables", {})
            variable_types = {}
            variable_domains = {}

            for var_name, var_info in variables.items():
                # Get variable type from attributes
                var_attrs = var_info.get("attributes", {})
                var_type = var_attrs.get("variable_type", "data")
                domain = var_attrs.get("domain", "unknown")

                variable_types[var_type] = variable_types.get(var_type, 0) + 1
                variable_domains[domain] = variable_domains.get(domain, 0) + 1

            # Analyze dimensions
            dimensions = group_data.get("dimensions", {})
            unlimited_dims = [
                dim_name
                for dim_name, dim_info in dimensions.items()
                if dim_info.get("unlimited", False)
            ]

            group_summary = {
                "path": current_path,
                "variables": {
                    "total": len(variables),
                    "by_type": variable_types,
                    "by_domain": variable_domains,
                    "names": list(variables.keys()),
                },
                "dimensions": {
                    "total": len(dimensions),
                    "unlimited": unlimited_dims,
                    "names": list(dimensions.keys()),
                },
                "subgroups": {
                    "total": len(group_data.get("subgroups", {})),
                    "names": list(group_data.get("subgroups", {}).keys()),
                },
            }

            # Recursively analyze subgroups
            subgroup_summaries = {}
            for subgroup_name, subgroup_info in group_data.get("subgroups", {}).items():
                subgroup_path = f"{path}/{subgroup_name}" if path else subgroup_name
                subgroup_summaries[subgroup_name] = analyze_group(
                    subgroup_info, subgroup_path
                )

            if subgroup_summaries:
                group_summary["subgroup_details"] = subgroup_summaries

            return group_summary

        return analyze_group(group_info)


@datasets_bp.route("", methods=["GET"])
@auth.login_required
def get_datasets() -> Response:
    """GET /datasets - List all datasets with optional filtering.

    Query Parameters:
    - name: Filter by dataset name (partial match, case-insensitive)
    - status: Filter by status
    - user: Filter by creator
    - created_after: Filter by creation date (ISO format)
    - created_before: Filter by creation date (ISO format)
    - limit: Limit number of results (default: 100, max: 1000)
    - offset: Pagination offset (default: 0)
    - format: Response format ('json' or 'dataframe')
    """
    try:
        # Parse query parameters
        filters = {
            "name": request.args.get("name"),
            "status": (
                request.args.get("status").capitalize()
                if request.args.get("status")
                else None
            ),
            "user": request.args.get("user"),
            "created_after": request.args.get("created_after"),
            "created_before": request.args.get("created_before"),
        }

        # Pagination parameters with validation
        try:
            limit = min(int(request.args.get("limit", 100)), 1000)
            if limit <= 0:
                limit = 100
        except (ValueError, TypeError):
            limit = 100

        try:
            offset = max(int(request.args.get("offset", 0)), 0)
        except (ValueError, TypeError):
            offset = 0

        format_type = request.args.get("format", "json").lower()

        # Build MongoDB query
        query = {}
        if filters["name"]:
            query["meta_name"] = {"$regex": filters["name"], "$options": "i"}
        if filters["status"]:
            query["system_status"] = filters["status"]
        if filters["user"]:
            query["system_user"] = filters["user"]
        if filters["created_after"] or filters["created_before"]:
            date_filter = {}
            if filters["created_after"]:
                date_filter["$gte"] = filters["created_after"]
            if filters["created_before"]:
                date_filter["$lte"] = filters["created_before"]
            query["system_date"] = date_filter

        manager = DatasetService.get_database_manager()
        total_count = manager.database_handler.file_collection.count_documents(query)
        cursor = manager.database_handler.file_collection.find(query)
        paginated_cursor = cursor.skip(offset).limit(limit)
        documents = list(paginated_cursor)
        datasets = [DatasetService.serialize_dataset(doc) for doc in documents]

        # Build response with pagination metadata
        response_data = {
            "datasets": datasets,
            "count": len(datasets),  # Number of items in current page
            "total_count": total_count,  # Total number of items matching filter
            "limit": limit,
            "offset": offset,
            "has_next": (offset + limit) < total_count,
            "has_previous": offset > 0,
            "next_offset": offset + limit if (offset + limit) < total_count else None,
            "previous_offset": max(0, offset - limit) if offset > 0 else None,
            "page": (offset // limit) + 1,  # Current page number (1-based)
            "total_pages": ((total_count - 1) // limit) + 1 if total_count > 0 else 0,
            "filters": {k: v for k, v in filters.items() if v is not None},
        }

        if format_type == "dataframe":
            import pandas as pd

            df = pd.DataFrame(datasets)
            response_data.update(
                {
                    "format": "dataframe",
                    "columns": df.columns.tolist() if not df.empty else [],
                    "data": df.to_dict("records"),
                }
            )

        return APIResponse.success(response_data)

    except Exception as e:
        return handle_api_error(e, "Failed to retrieve datasets")


@datasets_bp.route("/<uuid:dataset_id>", methods=["GET"])
@auth.login_required
def get_dataset(dataset_id: uuid.UUID) -> Response:
    """GET /datasets/{id} - Get a specific dataset by ID."""
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        dataset = DatasetService.serialize_dataset(document, include_full_data=True)
        return APIResponse.success({"dataset": dataset})

    except Exception as e:
        return handle_api_error(e, f"Failed to retrieve dataset {dataset_id}")


@datasets_bp.route("/search", methods=["POST"])
@auth.login_required
def search_datasets() -> Response:
    """POST /datasets/search - Advanced dataset search with complex filters."""
    try:
        search_data = request.get_json() or {}

        # Build complex query from search criteria
        query = {}

        # Text search
        if search_data.get("query"):
            query["$or"] = [
                {"meta_name": {"$regex": search_data["query"], "$options": "i"}},
                {"meta_description": {"$regex": search_data["query"], "$options": "i"}},
                {"meta_keywords": {"$in": [search_data["query"]]}},
            ]

        # Filters
        filters = search_data.get("filters", {})
        for field, value in filters.items():
            if field == "date_range":
                if value.get("start") or value.get("end"):
                    date_filter = {}
                    if value.get("start"):
                        date_filter["$gte"] = value["start"]
                    if value.get("end"):
                        date_filter["$lte"] = value["end"]
                    query["system_date"] = date_filter
            else:
                query[field] = value

        # Execute search
        manager = DatasetService.get_database_manager()
        documents = list(manager.database_handler.file_collection.find(query))

        datasets = [DatasetService.serialize_dataset(doc) for doc in documents]

        return APIResponse.success(
            {"datasets": datasets, "count": len(datasets), "query": search_data}
        )

    except Exception as e:
        return handle_api_error(e, "Search failed")


@datasets_bp.route("/<uuid:dataset_id>/metadata", methods=["GET"])
@auth.login_required
def get_dataset_metadata(dataset_id: uuid.UUID) -> Response:
    """GET /datasets/{id}/metadata - Get dataset metadata variables."""
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        metadata = DatasetService.serialize_dataset(document, include_full_data=True)

        return APIResponse.success({"metadata": metadata})

    except Exception as e:
        return handle_api_error(
            exception=e,
            message=f"Failed to retrieve metadata for dataset {dataset_id}",
        )


@datasets_bp.route("/<uuid:dataset_id>/variables", methods=["GET"])
@auth.login_required
def get_dataset_variables(dataset_id: uuid.UUID) -> Response:
    """GET /datasets/{id}/variables - Get all available variables in the dataset.

    Query Parameters:
    - group: Filter by group name
    - domain: Filter by domain
    - format: Response format ('json' or 'summary')
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        # Get variables using NetCDF4 handler
        handler = DatasetService.get_variable_handler()
        variables_info = handler.read_metadata_for_variables(result_filepath)

        # Apply filters
        group_filter = request.args.get("group")
        domain_filter = request.args.get("domain")
        format_type = request.args.get("format", "json").lower()

        if group_filter:
            variables_info = [
                v for v in variables_info if v.get("group") == group_filter
            ]
        if domain_filter:
            variables_info = [
                v for v in variables_info if v.get("domain") == domain_filter
            ]

        # Serialize variables
        variables = [
            DatasetService.serialize_variable_info(var) for var in variables_info
        ]

        response_data = {
            "dataset_id": str(dataset_id),
            "variables": variables,
            "count": len(variables),
        }

        if format_type == "summary":
            # Group variables by domain/group for summary
            summary = {}
            for var in variables:
                domain = var.get("domain", "unknown")
                group = var.get("group", "root")
                key = f"{domain}/{group}" if group != "root" else domain

                if key not in summary:
                    summary[key] = []
                summary[key].append(var["name"])

            response_data["summary"] = summary

        return APIResponse.success(response_data)

    except Exception as e:
        return handle_api_error(
            e, f"Failed to retrieve variables for dataset {dataset_id}"
        )


@datasets_bp.route("/<uuid:dataset_id>/variables/<variable_name>", methods=["GET"])
@auth.login_required
def get_variable_info(dataset_id: uuid.UUID, variable_name: str) -> Response:
    """Get detailed information about variable.

    GET /datasets/{id}/variables/{variable_name} -
    Get detailed information about a specific variable.
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        # Get variable info using NetCDF4 handler
        handler = DatasetService.get_variable_handler()

        try:
            variable_info = handler.get_variable_info(result_filepath, variable_name)

            if not variable_info:
                return APIResponse.not_found(f"Variable '{variable_name}' not found")

            # Get additional statistics
            stats = handler.get_variable_statistics(result_filepath, variable_name)

        except Exception as handler_error:
            logger.error(f"Handler error: {handler_error}")
            return APIResponse.error(
                f"Failed to access variable data: {str(handler_error)}", 500
            )

        # Ensure data is JSON serializable
        try:
            serialized_variable = DatasetService.serialize_variable_info(variable_info)
            serialized_stats = DatasetService.serialize_statistics(stats)
        except Exception as serialize_error:
            logger.error(f"JSON serialization error: {serialize_error}")
            return APIResponse.error("Failed to serialize variable data", 500)

        response_data = {
            "dataset_id": str(dataset_id),
            "variable": serialized_variable,
            "statistics": serialized_stats,
        }

        return APIResponse.success(response_data)

    except Exception as e:
        return handle_api_error(
            e, f"Failed to retrieve variable info for {variable_name}"
        )


@datasets_bp.route("/<uuid:dataset_id>/data", methods=["GET"])
@auth.login_required
def get_dataset_data(dataset_id: uuid.UUID) -> Response:
    """GET /datasets/{id}/data - Get data from the dataset.

    Query Parameters:
    - variables: Comma-separated list of variable names (required)
    - time_start: Start time index (optional)
    - time_end: End time index (optional)
    - time_indices: Comma-separated list of specific time indices (optional)
    - format: Response format ('json', 'csv', 'array')
    - include_time: Include time points data (default: true)
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        # Parse query parameters
        variables_param = request.args.get("variables")
        if not variables_param:
            return APIResponse.error("'variables' parameter is required", 400)

        variable_names = [v.strip() for v in variables_param.split(",")]
        time_start = request.args.get("time_start", type=int)
        time_end = request.args.get("time_end", type=int)
        time_indices_param = request.args.get("time_indices")
        format_type = request.args.get("format", "json").lower()
        include_time = request.args.get("include_time", "true").lower() == "true"

        # Parse time indices if provided
        time_indices = None
        if time_indices_param:
            time_indices = [int(i.strip()) for i in time_indices_param.split(",")]

        # Get data using NetCDF4 handler
        handler = DatasetService.get_variable_handler()

        data_result = {}

        # Get time points if requested
        if include_time:
            try:
                if time_indices:
                    time_data = handler.get_time_points_by_indices(
                        result_filepath, time_indices
                    )
                elif time_start is not None or time_end is not None:
                    time_data = handler.get_time_points_by_range(
                        result_filepath, time_start, time_end
                    )
                else:
                    time_data = handler.get_time_points(result_filepath)

                data_result["time_points"] = DatasetService.serialize_numpy_array(
                    time_data
                )

            except Exception as time_error:
                logger.warning(f"Failed to get time points: {time_error}")
                data_result["time_points"] = None

        # Get variable data
        for variable_name in variable_names:
            try:
                if time_indices:
                    var_data = handler.get_variable_data_by_indices(
                        result_filepath, variable_name, time_indices
                    )
                elif time_start is not None or time_end is not None:
                    var_data = handler.get_variable_data_by_range(
                        result_filepath, variable_name, time_start, time_end
                    )
                else:
                    var_data = handler.get_variable_data(result_filepath, variable_name)

                data_result[variable_name] = DatasetService.serialize_numpy_array(
                    var_data
                )

            except Exception as e:
                logger.warning(f"Failed to get data for variable {variable_name}: {e}")
                data_result[variable_name] = None

        response_data = {
            "dataset_id": str(dataset_id),
            "variables": variable_names,
            "data": data_result,
            "format": format_type,
        }

        # Handle different response formats
        if format_type == "csv":
            import pandas as pd
            import io

            # Convert to DataFrame and then CSV
            df_data = {}
            if (
                include_time
                and "time_points" in data_result
                and data_result["time_points"]
            ):
                df_data["time_points"] = data_result["time_points"]

            for var_name in variable_names:
                if data_result.get(var_name) is not None:
                    var_data = data_result[var_name]
                    # Handle different array dimensions
                    if isinstance(var_data, list) and len(var_data) > 0:
                        if isinstance(var_data[0], list):
                            # Multi-dimensional data - flatten or use first dimension
                            df_data[var_name] = [
                                row[0] if isinstance(row, list) else row
                                for row in var_data
                            ]
                        else:
                            df_data[var_name] = var_data

            if df_data:  # Only create DataFrame if we have data
                df = pd.DataFrame(df_data)
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)

                response_data.update(
                    {
                        "csv_data": csv_buffer.getvalue(),
                        "columns": df.columns.tolist(),
                    }
                )

        elif format_type == "array":
            # Return raw array format for easier processing
            response_data["arrays"] = {
                var: data_result[var]
                for var in variable_names
                if data_result.get(var) is not None
            }
            if include_time and data_result.get("time_points"):
                response_data["arrays"]["time_points"] = data_result.get("time_points")

        return APIResponse.success(response_data)

    except Exception as e:
        return handle_api_error(e, f"Failed to retrieve data from dataset {dataset_id}")


@datasets_bp.route("/<uuid:dataset_id>/data/<variable_name>", methods=["GET"])
@auth.login_required
def get_variable_data(dataset_id: uuid.UUID, variable_name: str) -> Response:
    """GET /datasets/{id}/data/{variable_name} - Get data for a specific variable.

    Query Parameters:
    - time_start: Start time index (optional)
    - time_end: End time index (optional)
    - time_indices: Comma-separated list of specific time indices (optional)
    - format: Response format ('json', 'csv', 'array')
    - include_stats: Include statistical summary (default: false)
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        # Parse query parameters
        time_start = request.args.get("time_start", type=int)
        time_end = request.args.get("time_end", type=int)
        time_indices_param = request.args.get("time_indices")
        format_type = request.args.get("format", "json").lower()
        include_stats = request.args.get("include_stats", "false").lower() == "true"

        # Parse time indices if provided
        time_indices = None
        if time_indices_param:
            time_indices = [int(i.strip()) for i in time_indices_param.split(",")]

        # Get data using NetCDF4 handler
        handler = DatasetService.get_variable_handler()

        # Get variable data
        try:
            if time_indices:
                var_data = handler.get_variable_data_by_indices(
                    result_filepath, variable_name, time_indices
                )
            elif time_start is not None or time_end is not None:
                var_data = handler.get_variable_data_by_range(
                    result_filepath, variable_name, time_start, time_end
                )
            else:
                var_data = handler.get_variable_data(result_filepath, variable_name)
        except Exception as data_error:
            logger.error(f"Failed to get variable data: {data_error}")
            return APIResponse.error(
                f"Failed to retrieve variable data: {str(data_error)}", 500
            )

        response_data = {
            "dataset_id": str(dataset_id),
            "variable_name": variable_name,
            "data": DatasetService.serialize_numpy_array(var_data),
            "format": format_type,
        }

        # Include statistics if requested
        if include_stats:
            try:
                stats = handler.get_variable_statistics(result_filepath, variable_name)
                response_data["statistics"] = DatasetService.serialize_statistics(stats)
            except Exception as stats_error:
                logger.warning(f"Failed to compute statistics: {stats_error}")
                response_data["statistics"] = {"error": str(stats_error)}

        # Handle CSV format
        if format_type == "csv":
            import pandas as pd
            import io

            data_array = DatasetService.serialize_numpy_array(var_data)
            if isinstance(data_array, list):
                df = pd.DataFrame({variable_name: data_array})
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                response_data["csv_data"] = csv_buffer.getvalue()

        return APIResponse.success(response_data)

    except Exception as e:
        return handle_api_error(
            e, f"Failed to retrieve data for variable {variable_name}"
        )


@datasets_bp.route("/<uuid:dataset_id>/groups", methods=["GET"])
@auth.login_required
def get_dataset_groups(dataset_id: uuid.UUID) -> Response:
    """GET /datasets/{id}/groups - Get all groups and their structure in the dataset.

    Query Parameters:
    - format: Response format ('full', 'flat', 'summary') - default: 'full'
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        # Get format parameter
        format_type = request.args.get("format", "full").lower()

        # Get group structure using NetCDF4 handler
        handler = DatasetService.get_variable_handler()
        groups_info = handler.get_group_structure(result_filepath)

        # Serialize based on format
        if format_type == "flat":
            serialized_groups = DatasetService.serialize_group_structure_flat(
                groups_info
            )
        elif format_type == "summary":
            serialized_groups = DatasetService.serialize_group_summary(groups_info)
        else:  # full
            serialized_groups = DatasetService.serialize_group_info(groups_info)

        return APIResponse.success(
            {
                "dataset_id": str(dataset_id),
                "format": format_type,
                "groups": serialized_groups,
            }
        )

    except Exception as e:
        return handle_api_error(
            e, f"Failed to retrieve groups for dataset {dataset_id}"
        )


@datasets_bp.route("/<uuid:dataset_id>/groups/<group_name>/variables", methods=["GET"])
@auth.login_required
def get_group_variables(dataset_id: uuid.UUID, group_name: str) -> Response:
    """Get variables in a specific group.

    GET /datasets/{id}/groups/{group_name}/variables -
    Get variables in a specific group.
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        handler = DatasetService.get_variable_handler()
        variables_info = handler.read_metadata_for_variables(
            netcdf4_file=result_filepath, group=group_name
        )

        variables = [
            DatasetService.serialize_variable_info(var) for var in variables_info
        ]

        return APIResponse.success(
            {
                "dataset_id": str(dataset_id),
                "group_name": group_name,
                "variables": variables,
                "count": len(variables),
            }
        )

    except Exception as e:
        return handle_api_error(
            e, f"Failed to retrieve variables for group {group_name}"
        )


@datasets_bp.route("/<uuid:dataset_id>/metadata_variables", methods=["GET"])
@auth.login_required
def get_metadata_variables(dataset_id: uuid.UUID) -> Response:
    """Route to get all meta_data variables.

    GET /datasets/{id}/metadata_variables - Get NetCDF4 variables of type 'meta_data'.
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        handler = DatasetService.get_variable_handler()
        meta_data_vars = handler.get_all_metadata_variables(result_filepath)

        return APIResponse.success(
            {
                "dataset_id": str(dataset_id),
                "meta_data_variables": meta_data_vars,
                "count": len(meta_data_vars),
            }
        )

    except Exception as e:
        return handle_api_error(
            e, f"Failed to retrieve meta_data variables for dataset {dataset_id}"
        )


@datasets_bp.route(
    "/<uuid:dataset_id>/metadata_variables/<metadata_variable_name>", methods=["GET"]
)
@auth.login_required
def get_metadata_variable(
    dataset_id: uuid.UUID, metadata_variable_name: str
) -> Response:
    """Route to get a specific meta_data variable.

    GET /datasets/{id}/metadata_variables/{metadata_variable_name}
    - Get a specific meta_data variable.
    """
    try:
        manager = DatasetService.get_database_manager()
        document = manager.get_database_entry_by_uuid(dataset_id)

        if not document:
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")
        if not result_filepath:
            return APIResponse.error("No result file available for this dataset", 400)

        handler = DatasetService.get_variable_handler()
        meta_data_vars = handler.get_all_metadata_variables(result_filepath)

        variable = next(
            (
                var
                for var in meta_data_vars
                if var["name"] == metadata_variable_name
                or var.get("full_path") == metadata_variable_name
            ),
            None,
        )

        if not variable:
            return APIResponse.not_found(
                f"Meta data variable '{metadata_variable_name}' not found"
            )

        return APIResponse.success(
            {
                "dataset_id": str(dataset_id),
                "meta_data_variable": variable,
            }
        )

    except Exception as e:
        return handle_api_error(
            e,
            (
                f"Failed to retrieve meta_data variable '{metadata_variable_name}' "
                f"for dataset {dataset_id}"
            ),
        )


@datasets_bp.route("/<uuid:dataset_id>/attributes", methods=["POST"])
@auth.login_required
def update_dataset_attributes(dataset_id: uuid.UUID) -> Response:
    """Update NetCDF4 dataset attributes (curators only).

    Updates both MongoDB and the NetCDF4 file to keep them in sync.
    """
    # Check if user is curator or admin
    current_user_data = get_current_user()
    roles = current_user_data.get("roles", [])

    if "curator" not in roles and "admin" not in roles:
        logger.warning(
            f"Unauthorized access attempt by user "
            f"{current_user_data.get('username', 'unknown')}"
        )
        return APIResponse.error("Unauthorized. Curator access required.", 403)

    try:
        # Get request data
        data: Dict[str, Any] = request.get_json()
        if not data:
            return APIResponse.error("No data provided", 400)

        meta_title = data.get("meta_title")  # Optional, not used currently
        meta_name = data.get("meta_name")
        meta_description = data.get("meta_description")

        logger.info(
            f"Received update request for dataset {dataset_id} with data: {data}"
        )

        # Validate required fields

        if (
            meta_title is None
            or not isinstance(meta_title, str)
            or not meta_title.strip()
        ):
            return APIResponse.error("meta_title is required and cannot be empty", 400)

        if meta_name is None or not isinstance(meta_name, str) or not meta_name.strip():
            return APIResponse.error("meta_name is required and cannot be empty", 400)

        if (
            meta_description is None
            or not isinstance(meta_description, str)
            or not meta_description.strip()
        ):
            return APIResponse.error(
                "meta_description is required and cannot be empty", 400
            )

        # Validate length

        if len(meta_title.strip()) > 50:
            return APIResponse.error(
                (
                    f"meta_title is too long ({len(meta_title.strip())} characters). "
                    "Maximum 50 characters allowed."
                ),
                400,
            )

        if len(meta_name.strip()) > 50:
            return APIResponse.error(
                (
                    f"meta_name is too long ({len(meta_name.strip())} characters). "
                    "Maximum 50 characters allowed."
                ),
                400,
            )

        if len(meta_description.strip()) > 200:
            return APIResponse.error(
                (
                    f"meta_description is too long ({len(meta_description.strip())} "
                    "characters). Maximum 200 characters allowed."
                ),
                400,
            )

        # Get database manager
        manager = DatasetService.get_database_manager()
        dataset_id_str = str(dataset_id)

        # Check if dataset exists
        document = manager.get_database_entry_by_uuid(dataset_id_str)
        if not document:
            logger.warning(f"Dataset {dataset_id_str} not found")
            return APIResponse.not_found("Dataset not found")

        result_filepath = document.get("system_result", "")

        # Prepare update data
        update_data = {
            "meta_title": meta_title.strip(),
            "meta_name": meta_name.strip(),
            "meta_description": meta_description.strip(),
        }

        # NEW: Uniqueness restrictions for meta_title and meta_name (exclude self)
        try:
            exclude_self = {"system_uuid": {"$ne": dataset_id_str}}

            title_re = {
                "$regex": f"^{re.escape(update_data['meta_title'])}$",
                "$options": "i",
            }
            name_re = {
                "$regex": f"^{re.escape(update_data['meta_name'])}$",
                "$options": "i",
            }

            projection = {"system_uuid": 1, "meta_title": 1, "meta_name": 1}

            conflict_title = manager.database_handler.file_collection.find_one(
                {**exclude_self, "meta_title": title_re},
                projection,
            )
            conflict_name = manager.database_handler.file_collection.find_one(
                {**exclude_self, "meta_name": name_re},
                projection,
            )

            if conflict_title or conflict_name:
                problems: list[str] = []

                if conflict_title:
                    other_uuid = conflict_title.get("system_uuid")
                    other_name = conflict_title.get("meta_name")
                    suffix = f" (meta_name: {other_name})" if other_name else ""
                    problems.append(
                        f"meta_title '{update_data['meta_title']}' "
                        f"is already used by dataset {other_uuid}.{suffix}"
                    )

                if conflict_name:
                    other_uuid = conflict_name.get("system_uuid")
                    other_title = conflict_name.get("meta_title")
                    suffix = f" (meta_title: {other_title})" if other_title else ""
                    problems.append(
                        f"meta_name '{update_data['meta_name']}' "
                        f"is already used by dataset {other_uuid}.{suffix}"
                    )

                logger.warning(
                    f"Uniqueness constraint violation for dataset "
                    f"{dataset_id_str}: {'; '.join(problems)}"
                )

                return APIResponse.error(" ".join(problems), 409)

        except Exception as db_check_error:
            logger.error(
                f"Failed to validate uniqueness for dataset "
                f"{dataset_id_str}: {db_check_error}.",
                exc_info=True,
            )
            return APIResponse.error("Failed to validate uniqueness constraints", 500)

        logger.info(f"Updating dataset {dataset_id_str} with data: {update_data}")

        # STEP 1: Update NetCDF4 file first (atomic operation)
        if result_filepath:
            try:
                with nc.Dataset(result_filepath, "a") as ncfile:
                    # Update global attributes
                    ncfile.title = meta_title.strip()
                    ncfile.technical_name = meta_name.strip()
                    ncfile.description = meta_description.strip()
                    # Add timestamp of last modification
                    ncfile.last_modified = datetime.now(timezone.utc).isoformat()
                    ncfile.last_modified_by = current_user_data.get(
                        "username", "unknown"
                    )

                logger.info(f"Successfully updated NetCDF4 file: {result_filepath}")

            except Exception as nc_error:
                logger.error(
                    f"Failed to update NetCDF4 file: {nc_error}", exc_info=True
                )
                return APIResponse.error(
                    f"Failed to update NetCDF4 file: {str(nc_error)}", 500
                )
        else:
            logger.warning(f"No result file path found for dataset {dataset_id_str}")

        # STEP 2: Update MongoDB (only if NetCDF4 update succeeded or no file exists)
        try:
            result = manager.database_handler.file_collection.update_one(
                {"system_uuid": dataset_id_str},
                {
                    "$set": {
                        **update_data,
                        "system_last_modified": datetime.now(timezone.utc).isoformat(),
                        "system_last_modified_by": current_user_data.get(
                            "username", "unknown"
                        ),
                    }
                },
            )

            logger.info(
                f"MongoDB update result: matched={result.matched_count}, "
                f"modified={result.modified_count}"
            )

            if result.matched_count > 0:
                # Fetch updated document to verify
                updated_document = manager.get_database_entry_by_uuid(dataset_id_str)
                logger.info(f"Successfully updated dataset {dataset_id_str}")

                return APIResponse.success(
                    {
                        "message": (
                            "Dataset attributes updated successfully "
                            "in both MongoDB and NetCDF4 file"
                        ),
                        "dataset": DatasetService.serialize_dataset(
                            updated_document, include_full_data=True
                        ),
                        "update_info": {
                            "matched_count": result.matched_count,
                            "modified_count": result.modified_count,
                            "netcdf4_updated": bool(result_filepath),
                        },
                    }
                )
            else:
                logger.error(f"No document matched for dataset {dataset_id_str}")
                return APIResponse.error(
                    "Failed to update dataset - no matching document found", 404
                )

        except Exception as db_error:
            logger.error(f"Failed to update MongoDB: {db_error}", exc_info=True)
            # If MongoDB update fails but NetCDF4 succeeded, we have inconsistency
            # You might want to rollback the NetCDF4 change or log this
            # for manual intervention
            return APIResponse.error(
                (
                    f"Failed to update MongoDB "
                    f"(NetCDF4 file may be out of sync): {str(db_error)}"
                ),
                500,
            )

    except Exception as e:
        logger.error(f"Error updating dataset attributes: {str(e)}", exc_info=True)
        return APIResponse.error(f"Internal server error: {str(e)}", 500)
