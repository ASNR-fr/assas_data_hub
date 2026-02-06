"""Script to restore collections using AssasDatabaseHandler.restore_collections method.
Added: hard-restore mode which drops the target database before restoring so the
database exactly matches the backup (hard restore). Supports CLI flags.
"""

import sys
import os
import logging
import argparse
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from assasdb import AssasDatabaseHandler
from pymongo import MongoClient

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def hard_drop_database(client: MongoClient, database_name: str, auto_confirm: bool = False) -> bool:
    """Drop the entire database after confirmation."""
    if not auto_confirm:
        confirm = input(
            f"WARNING: This will DROP the entire database '{database_name}' permanently. Type 'YES' to continue: "
        ).strip()
        if confirm != "YES":
            logger.info("Hard drop cancelled by user.")
            return False

    try:
        logger.info(f"Dropping database: {database_name}")
        client.drop_database(database_name)
        logger.info("Database dropped successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to drop database {database_name}: {e}")
        return False


def restore_files_collection(backup_directory: str, connection_string: str, database_name: str,
                             collection_name: str, hard: bool = False, auto_confirm: bool = False) -> bool:
    """Restore the files collection from backup using AssasDatabaseHandler. Optionally perform hard drop first."""
    logger.info("Starting collection restore process...")
    logger.info(f"Backup directory: {backup_directory}")
    logger.info(f"Target database: {database_name}")
    logger.info(f"Target collection: {collection_name}")
    logger.info(f"Hard restore: {hard}")

    backup_path = Path(backup_directory)
    if not backup_path.exists():
        logger.error(f"Backup directory does not exist: {backup_directory}")
        return False

    # Basic check for presence of bson files
    bson_files = list(backup_path.glob("*.bson"))
    if not bson_files:
        logger.error(f"No .bson files found in backup directory: {backup_directory}")
        return False

    try:
        client = MongoClient(connection_string)
        client.admin.command("ping")
        logger.info("MongoDB connection successful")

        if hard:
            ok = hard_drop_database(client, database_name, auto_confirm=auto_confirm)
            if not ok:
                logger.error("Hard drop aborted, cancelling restore.")
                return False

        # Check collection count before restore (after potential drop)
        db = client[database_name]
        collection = db[collection_name]
        count_before = collection.count_documents({})
        logger.info(f"Documents in collection before restore: {count_before}")

        # Use AssasDatabaseHandler to perform restore (assumes it uses backup_directory)
        logger.info("Initializing AssasDatabaseHandler (restore will be triggered)...")
        db_handler = AssasDatabaseHandler(
            client=client,
            backup_directory=backup_directory,
            database_name=database_name,
            file_collection_name=collection_name,
            restore_from_backup=True,  # This triggers restore_collections() in __init__
        )

        # Re-check counts after restore
        count_after = collection.count_documents({})
        new_documents = count_after - count_before
        logger.info("Restore completed!")
        logger.info(f"Documents before restore: {count_before}")
        logger.info(f"Documents after restore: {count_after}")
        logger.info(f"New documents added: {new_documents}")

        db_handler.close()
        return True

    except Exception as e:
        logger.error(f"Error during restore: {e}")
        return False


def restore_manually(backup_directory: str, connection_string: str, database_name: str,
                     collection_name: str, hard: bool = False, auto_confirm: bool = False) -> bool:
    """Manually call restore_collections method without using __init__ flag. Optionally perform hard drop first."""
    try:
        client = MongoClient(connection_string)
        client.admin.command("ping")
        logger.info("MongoDB connection successful")

        if hard:
            ok = hard_drop_database(client, database_name, auto_confirm=auto_confirm)
            if not ok:
                logger.error("Hard drop aborted, cancelling manual restore.")
                return False

        db = client[database_name]
        collection = db[collection_name]
        count_before = collection.count_documents({})
        logger.info(f"Documents in collection before restore: {count_before}")

        logger.info("Creating AssasDatabaseHandler (no auto-restore)...")
        db_handler = AssasDatabaseHandler(
            client=client,
            backup_directory=backup_directory,
            database_name=database_name,
            file_collection_name=collection_name,
            restore_from_backup=False,
        )

        logger.info("Calling restore_collections() manually...")
        db_handler.restore_collections()

        count_after = collection.count_documents({})
        new_documents = count_after - count_before

        logger.info("Manual restore completed!")
        logger.info(f"Documents before restore: {count_before}")
        logger.info(f"Documents after restore: {count_after}")
        logger.info(f"New documents added: {new_documents}")

        db_handler.close()
        return True

    except Exception as e:
        logger.error(f"Error during manual restore: {e}")
        return False


def show_backup_info(backup_directory: str):
    """Show information about available backup files."""
    backup_path = Path(backup_directory)
    if not backup_path.exists():
        logger.error(f"Backup directory does not exist: {backup_directory}")
        return

    logger.info(f"Backup directory: {backup_directory}")
    logger.info("Available backup files:")
    bson_files = list(backup_path.glob("*.bson"))
    if not bson_files:
        logger.warning("No .bson files found in backup directory")
        return

    for bson_file in bson_files:
        file_size = bson_file.stat().st_size
        logger.info(f"  - {bson_file.name}: {file_size:,} bytes")


def parse_args():
    p = argparse.ArgumentParser(description="ASSAS Database Collection Restore Script")
    p.add_argument("--backup-dir", default="/mnt/ASSAS/backup_mongodb", help="Backup directory containing .bson files")
    p.add_argument("--conn", default="mongodb://localhost:27017/", help="MongoDB connection string")
    p.add_argument("--db", default="assas_dev", help="Target database name")
    p.add_argument("--collection", default="files", help="Target collection name to restore")
    p.add_argument("--hard", action="store_true", help="Perform a hard restore: drop the target database before restore")
    p.add_argument("--yes", action="store_true", help="Automatic yes for prompts (use with --hard to skip confirmation)")
    p.add_argument("--manual", action="store_true", help="Use manual restore flow (calls restore_collections explicitly)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("ASSAS Database Collection Restore Script")
    print("=" * 50)

    show_backup_info(args.backup_dir)

    if args.manual:
        logger.info("Starting manual restore (CLI mode)...")
        success = restore_manually(
            backup_directory=args.backup_dir,
            connection_string=args.conn,
            database_name=args.db,
            collection_name=args.collection,
            hard=args.hard,
            auto_confirm=args.yes,
        )
    else:
        # Interactive menu only when no CLI flags provided beyond defaults
        if len(sys.argv) > 1:
            # Non-interactive: run auto-restore (default) with provided flags
            logger.info("Starting auto-restore (CLI mode)...")
            success = restore_files_collection(
                backup_directory=args.backup_dir,
                connection_string=args.conn,
                database_name=args.db,
                collection_name=args.collection,
                hard=args.hard,
                auto_confirm=args.yes,
            )
        else:
            # Interactive menu
            print("\nChoose restore method:")
            print("1. Auto-restore (using restore_from_backup=True in constructor)")
            print("2. Manual restore (calling restore_collections() explicitly)")
            print("3. HARD restore (drop DB then restore)")

            choice = input("Enter choice (1, 2 or 3): ").strip()

            if choice == "1":
                logger.info("Starting auto-restore...")
                success = restore_files_collection(
                    backup_directory=args.backup_dir,
                    connection_string=args.conn,
                    database_name=args.db,
                    collection_name=args.collection,
                    hard=False,
                    auto_confirm=False,
                )
            elif choice == "2":
                logger.info("Starting manual restore...")
                success = restore_manually(
                    backup_directory=args.backup_dir,
                    connection_string=args.conn,
                    database_name=args.db,
                    collection_name=args.collection,
                    hard=False,
                    auto_confirm=False,
                )
            elif choice == "3":
                logger.info("Starting HARD restore (will drop database)...")
                success = restore_files_collection(
                    backup_directory=args.backup_dir,
                    connection_string=args.conn,
                    database_name=args.db,
                    collection_name=args.collection,
                    hard=True,
                    auto_confirm=False,
                )
            else:
                logger.info("Starting auto-restore (default)...")
                success = restore_files_collection(
                    backup_directory=args.backup_dir,
                    connection_string=args.conn,
                    database_name=args.db,
                    collection_name=args.collection,
                    hard=False,
                    auto_confirm=False,
                )

    if success:
        print("\n✅ Restore completed successfully!")
    else:
        print("\n❌ Restore failed!")
