import pathlib
import sys
import logging
from logging.handlers import RotatingFileHandler
import os
import time
from dotenv import load_dotenv
import dap.integration.database_errors
from dap.dap_types import Credentials
import asyncio
from dap.api import DAPClient
from dap.integration.database import DatabaseConnection
from dap.replicator.sql import SQLReplicator
import sqlalchemy
import smtplib
from email.mime.text import MIMEText
from pathlib import Path

load_dotenv()
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("dap")
logger.setLevel(logging.INFO)
if "DEBUG" in sys.argv:
    logger.setLevel(logging.DEBUG)
formatter = logging.Formatter()
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
handler = logging.StreamHandler(sys.stdout)
fileHandler = RotatingFileHandler('./canvas_auto_shifter.log', maxBytes=2000000000, backupCount=10)
fileHandler.setFormatter(logFormatter)
logger.addHandler(handler)
logger.addHandler(fileHandler)

base_url: str = os.environ["DAP_API_URL"]
client_id: str = os.environ["DAP_CLIENT_ID"]
client_secret: str = os.environ["DAP_CLIENT_SECRET"]
connection_string: str = os.environ["DAP_CONNECTION_STRING"]
sender: str = os.environ["SENDER"]
recipient: str = os.environ["RECIPIENT"]

credentials = Credentials.create(client_id=client_id, client_secret=client_secret)

# Fetches the list of tables for a given namespace using DAPClient
async def get_dap_tables(namespace, creds) -> list:
    async with DAPClient(credentials=creds) as session:
        return await session.get_tables(namespace=namespace)

# Initializes a table in the database for synchronization, retries if an error occurs
async def init_table_db_sync(table_name, namespace):
    db_connection = DatabaseConnection(connection_string)
    async with DAPClient(credentials=credentials) as session:
        try:
            sql = SQLReplicator(session, db_connection)
            await sql.initialize(namespace, table_name)
        except ValueError as ve:
            if "table already replicated, use `syncdb`" in ve.args:
                logger.info(f"Table {table_name} already initialized")
            else:
                raise ve
        except Exception as e:
            logger.error(f"Initialization failed for table {table_name}: {e}")
            logger.info(f"Attempting to delete and retry initialization for table {table_name}")
            try:
                await db_connection.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                await sql.version_upgrade()
                await sql.initialize(namespace, table_name)
                logger.info(f"Retry initialization completed for table {table_name}")
            except Exception as retry_e:
                logger.error(f"Retry initialization failed for table {table_name}: {retry_e}")
                raise retry_e

# Synchronizes a table in the database
async def sync_table_db_sync(table_name, namespace, error_messages):
    db_connection = DatabaseConnection(connection_string)
    async with DAPClient(base_url, credentials) as session:
        try:
            sql = SQLReplicator(session, db_connection)
            await sql.version_upgrade()
            await sql.synchronize(namespace, table_name)
        except Exception as e:
            error_message = f"Sync failed for table {table_name}: {e}"
            logger.error(error_message)
            error_messages.append(error_message)

# Processes a namespace by initializing and/or synchronizing its tables
async def process_namespace(nspace, args, failed_tables, error_messages):
    eng = sqlalchemy.create_engine(connection_string)
    inspector = sqlalchemy.inspect(eng)
    existing_tables = inspector.get_table_names(nspace)

    table_list = await get_dap_tables(nspace, credentials)

    if "init" in args:
        for table_name in table_list:
            if table_name in existing_tables:
                logger.info(f"Skipping initialization of existing table {table_name}")
                continue
            logger.info(f'Init Beginning: {table_name}')
            try:
                await init_table_db_sync(table_name, nspace)
                logger.info(f'Init Completed: {table_name}')
            except dap.integration.database_errors.TableAlreadyExistsError:
                logger.debug(f'Table {table_name} already exists, skipping initialization.')
    if "sync" in args:
        for table_name in table_list:
            logger.info(f'Sync Beginning: {table_name}')
            await sync_table_db_sync(table_name, nspace, error_messages)
            logger.info(f'Sync Completed: {table_name}')

# Sends an email with the list of tables that failed to sync and the total time taken
def send_failure_email(failed_tables, error_messages, total_time):
    if not failed_tables:
        return

    total_time_minutes = total_time / 60  # Convert time to minutes

    subject = "Canvas Sync Failure Report"
    body = (
        "The following tables have failed to sync:\n\n"
        + "\n".join(failed_tables)
        + "\n\nError messages:\n\n"
        + "\n".join(error_messages)
        + f"\n\nThe total time took to sync was: {total_time_minutes:.2f} minutes"
    )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    try:
        with smtplib.SMTP("mail.cedarville.edu", 25) as server:
            server.sendmail(sender, recipient, msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error: {e}")

# Main function that processes namespaces and sends failure email if needed
async def main(args):
    namespaces = ["canvas", "canvas_logs"]
    failed_tables = []
    error_messages = []
    start_time = time.time()

    for nspace in namespaces:
        if nspace == "canvas" and "main" in args:
            await process_namespace(nspace, args, failed_tables, error_messages)
        elif nspace == "canvas_logs" and "logs" in args:
            await process_namespace(nspace, args, failed_tables, error_messages)

    total_time = time.time() - start_time
    send_failure_email(failed_tables, error_messages, total_time)

if __name__ == "__main__":
    if 'seq' in sys.argv:
        asyncio.run(main(sys.argv))
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(sys.argv))
        loop.close()
    Path("./canvas_auto_shifter_complete").touch()
