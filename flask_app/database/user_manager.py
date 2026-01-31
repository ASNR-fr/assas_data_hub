"""User Management for MongoDB operations."""

import logging
import os

from types import TracebackType
from typing import List, Dict, Optional, Any, Union
from typing_extensions import Self
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from flask import current_app
from dotenv import load_dotenv

logger = logging.getLogger("assas_app")


class UserManager:
    """Manage user operations in MongoDB."""

    def __init__(self) -> None:
        """Initialize UserManager with MongoDB connection from config."""
        try:
            # Get configuration from Flask app or environment
            if current_app:
                # Use Flask app configuration
                connection_string = current_app.config.get("CONNECTIONSTRING", "")
                db_name = current_app.config.get("MONGO_DB_NAME", "assas_dev")
                collection_name = current_app.config.get(
                    "USER_COLLECTION_NAME", "users"
                )
                logger.info("Using Flask app configuration")
            else:
                # Fall back to environment variables or defaults
                load_dotenv()

                connection_string = os.getenv("CONNECTIONSTRING", "")
                db_name = os.getenv("MONGO_DB_NAME", "assas_dev")
                collection_name = os.getenv("USER_COLLECTION_NAME", "users")
                logger.info("Using environment configuration")

            logger.info(f"Connecting to MongoDB: {connection_string}")
            logger.info(f"Database: {db_name}")
            logger.info(f"Collection: {collection_name}")

            self.client = MongoClient(connection_string)
            self.db = self.client[db_name]
            self.users_collection = self.db[collection_name]

            # Check connection without creating conflicting indexes
            self.client.admin.command("ping")
            logger.info(f"Connected to MongoDB: {connection_string}")

            # Only create indexes if they don't exist or are different
            self._ensure_indexes()

        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def _ensure_indexes(self) -> None:
        """Ensure required indexes exist, handling conflicts gracefully."""
        try:
            existing_indexes = list(self.users_collection.list_indexes())

            # Check if we need to create unique email index
            email_index_exists = any(
                idx.get("key", {}).get("email") == 1 and idx.get("unique", False)
                for idx in existing_indexes
            )

            if not email_index_exists:
                try:
                    self.users_collection.create_index(
                        "email", unique=True, name="email_unique_idx"
                    )
                    logger.info("Created unique email index")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"Could not create email index: {e}")

            # Check if we need to create unique username index
            username_index_exists = any(
                idx.get("key", {}).get("username") == 1 and idx.get("unique", False)
                for idx in existing_indexes
            )

            if not username_index_exists:
                try:
                    self.users_collection.create_index(
                        "username", unique=True, name="username_unique_idx"
                    )
                    logger.info("Created unique username index")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"Could not create username index: {e}")

        except Exception as e:
            logger.warning(f"Could not ensure indexes: {e}")
            # Don't raise - the app can work without perfect indexes

    def get_all_users(self) -> List[Dict]:
        """Get all users from database with ObjectId converted to string."""
        try:
            logger.info("Starting get_all_users() method")
            logger.info(f"Collection: {self.users_collection}")
            logger.info(f"Database: {self.db}")

            # First, let's check if the collection exists and has documents
            collection_names = self.db.list_collection_names()
            logger.info(f"Available collections: {collection_names}")

            if "users" not in collection_names:
                logger.error("Users collection does not exist!")
                return []

            # Count documents first
            count = self.users_collection.count_documents({})
            logger.info(f"Total documents in users collection: {count}")

            if count == 0:
                logger.warning("Users collection is empty!")
                return []

            # Get the users
            logger.info("Executing find() query...")
            users_cursor = self.users_collection.find({})
            users = list(users_cursor)

            logger.info(f"Retrieved {len(users)} users from database")
            logger.info(
                f"First user keys (if any): "
                f"{list(users[0].keys()) if users else 'No users'}"
            )

            # Convert ObjectId to string for all users
            converted_users = [self._convert_objectid_to_string(user) for user in users]

            return converted_users

        except Exception as e:
            logger.error(f"Error retrieving users: {e}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def _convert_objectid_to_string(
        self, obj: Union[ObjectId, Dict, List]
    ) -> Union[Dict, List, str]:
        """Convert ObjectId objects to strings recursively."""
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {
                key: self._convert_objectid_to_string(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [self._convert_objectid_to_string(item) for item in obj]
        else:
            return obj

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user by email address."""
        try:
            user = self.users_collection.find_one({"email": email.lower()})
            if user:
                # Convert ObjectId to string for JSON serialization
                return self._convert_objectid_to_string(user)
            return None
        except Exception as e:
            logger.error(f"Error getting user by email: {e}")
            return None

    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user by username."""
        try:
            user = self.users_collection.find_one({"username": username})
            if user:
                # Convert ObjectId to string for JSON serialization
                return self._convert_objectid_to_string(user)
            return None
        except Exception as e:
            logger.error(f"Error getting user by username: {e}")
            return None

    def get_users_with_basic_auth(self) -> List[Dict]:
        """Get all users with basic auth enabled with ObjectId converted to string."""
        try:
            users = list(
                self.users_collection.find(
                    {"basic_auth_password_hash": {"$exists": True, "$ne": None}}
                )
            )
            logger.info(f"Found {len(users)} users with basic auth")

            # Convert ObjectId to string for all users
            converted_users = [self._convert_objectid_to_string(user) for user in users]

            return converted_users
        except Exception as e:
            logger.error(f"Error retrieving basic auth users: {e}")
            return []

    def create_or_update_user(self, user_data: Dict) -> Dict:
        """Create new user or update existing user."""
        try:
            email = user_data.get("email")
            username = user_data.get("username")

            if not email:
                raise ValueError("Email is required for user creation")

            # Check if user exists (by email or username for basic auth)
            existing_user = None
            if username and user_data.get("provider") == "basic_auth":
                existing_user = self.get_user_by_username(username)
            if not existing_user:
                existing_user = self.get_user_by_email(email)

            # Prepare user document
            user_doc = {
                "username": username,
                "email": email,
                "name": user_data.get("name"),
                "provider": user_data.get("provider"),
                "roles": user_data.get("roles", ["visitor"]),
                "is_active": user_data.get("is_active", True),
                "last_login": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }

            if user_data.get("provider") == "helmholtz":
                user_doc.update(
                    {
                        "helmholtz_sub": user_data.get("helmholtz_sub"),
                        "institution": user_data.get("institution"),
                        "entitlements": user_data.get("entitlements", []),
                        "affiliations": user_data.get("affiliations", []),
                    }
                )
            elif user_data.get("provider") == "basic_auth":
                # Add basic auth specific fields
                if user_data.get("basic_auth_password_hash"):
                    user_doc["basic_auth_password_hash"] = user_data[
                        "basic_auth_password_hash"
                    ]

            if existing_user:
                # Update existing user
                user_doc["login_count"] = existing_user.get("login_count", 0) + 1

                # Convert existing_user _id to ObjectId for the query
                user_id = existing_user["_id"]
                if isinstance(user_id, str):
                    user_id = ObjectId(user_id)

                result = self.users_collection.update_one(
                    {"_id": user_id}, {"$set": user_doc}
                )

                if result.modified_count > 0:
                    logger.info(f"Updated user: {email}")
                    return self.get_user_by_email(email)
                else:
                    logger.warning(f"No changes made to user: {email}")
                    return existing_user
            else:
                # Create new user
                user_doc.update({"created_at": datetime.utcnow(), "login_count": 1})

                result = self.users_collection.insert_one(user_doc)
                logger.info(f"Created new user: {email} with ID: {result.inserted_id}")

                return self.get_user_by_id(result.inserted_id)

        except Exception as e:
            logger.error(f"Error creating/updating user: {e}")
            raise

    def update_basic_auth_password(self, username: str, password_hash: str) -> bool:
        """Update basic auth password for user."""
        try:
            result = self.users_collection.update_one(
                {"username": username},
                {
                    "$set": {
                        "basic_auth_password_hash": password_hash,
                        "updated_at": datetime.utcnow(),
                    }
                },
            )

            if result.modified_count > 0:
                logger.info(f"Updated basic auth password for user: {username}")
                return True
            else:
                logger.warning(f"No password update for user: {username}")
                return False

        except Exception as e:
            logger.error(f"Error updating basic auth password for {username}: {e}")
            return False

    def update_last_login(self, username: str) -> bool:
        """Update last login timestamp."""
        try:
            result = self.users_collection.update_one(
                {"username": username},
                {"$set": {"last_login": datetime.utcnow()}, "$inc": {"login_count": 1}},
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating last login for {username}: {e}")
            return False

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID with ObjectId converted to string."""
        try:
            if isinstance(user_id, str):
                user_id = ObjectId(user_id)
            user = self.users_collection.find_one({"_id": user_id})
            if user:
                # Convert ObjectId to string for JSON serialization
                return self._convert_objectid_to_string(user)
            return None
        except Exception as e:
            logger.error(f"Error getting user by ID: {e}")
            return None

    def create_user(self, user_data: Dict[str, Any]) -> bool:
        """Create a new user in the database."""
        try:
            # Validate required fields
            required_fields = ["username", "email", "provider", "roles"]
            for field in required_fields:
                if field not in user_data:
                    logger.error(f"Missing required field: {field}")
                    return False

            # Check for existing username
            if self.get_user_by_username(user_data["username"]):
                logger.error(f"Username {user_data['username']} already exists")
                return False

            # Check for existing email
            if self.get_user_by_email(user_data["email"]):
                logger.error(f"Email {user_data['email']} already exists")
                return False

            # Insert user
            result = self.users_collection.insert_one(user_data)

            if result.inserted_id:
                logger.info(
                    f"Created user: {user_data['username']} "
                    f"with ID: {result.inserted_id}"
                )
                return True
            else:
                logger.error("Failed to insert user")
                return False

        except Exception as e:
            logger.error(f"Error creating user: {e}")
            return False

    def delete_user(self, user_id: str) -> bool:
        """Delete a user from the database."""
        try:
            if isinstance(user_id, str):
                user_id = ObjectId(user_id)

            # Get user info before deletion for logging
            user = self.get_user_by_id(user_id)
            if not user:
                logger.error(f"User with ID {user_id} not found")
                return False

            # Prevent deletion of the last admin user
            if "admin" in user.get("roles", []):
                admin_count = self.users_collection.count_documents({"roles": "admin"})
                if admin_count <= 1:
                    logger.error("Cannot delete the last admin user")
                    return False

            # Delete the user
            result = self.users_collection.delete_one({"_id": user_id})

            if result.deleted_count > 0:
                logger.info(
                    f"Deleted user: {user.get('username', 'unknown')} (ID: {user_id})"
                )
                return True
            else:
                logger.error(f"Failed to delete user with ID: {user_id}")
                return False

        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            return False

    def soft_delete_user(self, user_id: str) -> bool:
        """Soft delete a user (mark as inactive instead of deleting)."""
        try:
            if isinstance(user_id, str):
                user_id = ObjectId(user_id)

            result = self.users_collection.update_one(
                {"_id": user_id},
                {
                    "$set": {
                        "is_active": False,
                        "deleted_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                    }
                },
            )

            if result.modified_count > 0:
                logger.info(f"Soft deleted user with ID: {user_id}")
                return True
            else:
                logger.error(f"Failed to soft delete user with ID: {user_id}")
                return False

        except Exception as e:
            logger.error(f"Error soft deleting user: {e}")
            return False

    def update_user_by_id(
        self, user_id: str, updates: Dict[str, Any]
    ) -> Optional[Dict]:
        """Update a user by ID and return the updated user."""
        try:
            if not updates:
                logger.warning("No updates provided")
                return None

            if isinstance(user_id, str):
                user_id = ObjectId(user_id)

            current_user = self.users_collection.find_one({"_id": user_id})
            if not current_user:
                logger.error(f"User with ID {user_id} not found")
                return None

            # Enforce unique email/username if changed
            if "email" in updates:
                existing = self.get_user_by_email(updates["email"])
                if existing and existing["_id"] != str(user_id):
                    logger.error("Email already in use")
                    return None

            if "username" in updates:
                existing = self.get_user_by_username(updates["username"])
                if existing and existing["_id"] != str(user_id):
                    logger.error("Username already in use")
                    return None

            # Protect last admin from losing admin role or being deactivated
            if "admin" in current_user.get("roles", []):
                admin_count = self.users_collection.count_documents({"roles": "admin"})
                if admin_count <= 1:
                    roles = updates.get("roles", current_user.get("roles", []))
                    is_active = updates.get(
                        "is_active", current_user.get("is_active", True)
                    )
                    if "admin" not in roles or is_active is False:
                        logger.error("Cannot remove/deactivate the last admin user")
                        return None

            updates["updated_at"] = datetime.utcnow()
            logger.info(f"Updating user ID !!! {user_id} with updates: {updates}")
            result = self.users_collection.update_one(
                {"_id": user_id}, {"$set": updates}
            )
            if result.modified_count > 0:
                return self.get_user_by_id(str(user_id))

            logger.warning("No changes made to user")
            return self.get_user_by_id(str(user_id))

        except Exception as e:
            logger.error(f"Error updating user by ID: {e}")
            return None

    def unset_user_field_by_id(self, user_id: str, field_name: str) -> bool:
        """Unset (delete) a field from a user document by ID.

        This is intended for removing custom properties via MongoDB `$unset`.
        Returns True if the user exists and the operation succeeded.
        """
        try:
            if not field_name or not isinstance(field_name, str):
                logger.error("Field name is required for unset")
                return False

            field = field_name.strip()
            if (
                not field
                or field.startswith("$")
                or "." in field
                or field.startswith("_")
            ):
                logger.error(f"Invalid field name for unset: {field_name}")
                return False

            # Protect critical fields from being removed
            protected = {
                "_id",
                "id",
                "username",
                "email",
                "provider",
                "roles",
                "is_active",
                "created_at",
                "updated_at",
                "last_login",
                "login_count",
                "basic_auth_password_hash",
                "temp_basic_auth_password_hash",
            }
            if field in protected:
                logger.error(f"Refusing to unset protected field: {field}")
                return False

            obj_id = ObjectId(user_id) if isinstance(user_id, str) else user_id

            # Ensure user exists
            current_user = self.users_collection.find_one({"_id": obj_id})
            if not current_user:
                logger.error(f"User with ID {user_id} not found")
                return False

            result = self.users_collection.update_one(
                {"_id": obj_id},
                {"$unset": {field: ""}, "$set": {"updated_at": datetime.utcnow()}},
            )

            # modified_count can be 0 if the field didn't exist;
            # matched_count indicates success.
            return result.matched_count > 0

        except Exception as e:
            logger.error(
                f"Error unsetting field '{field_name}' for user {user_id}: {e}"
            )
            return False

    def close(self) -> None:
        """Close the MongoDB connection."""
        try:
            if hasattr(self, "client"):
                self.client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.warning(f"Error closing MongoDB connection: {e}")

    def __enter__(self) -> Self:
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit."""
        self.close()


# Utility function to create UserManager with proper error handling
def get_user_manager() -> UserManager:
    """Get a UserManager instance with proper error handling."""
    try:
        return UserManager()
    except Exception as e:
        logger.error(f"Failed to create UserManager: {e}")
        raise


# Optional: Create a singleton pattern for UserManager if needed
_user_manager_instance = None


def get_user_manager_singleton() -> UserManager:
    """Get a singleton UserManager instance."""
    global _user_manager_instance
    if _user_manager_instance is None:
        _user_manager_instance = UserManager()
    return _user_manager_instance
