import sys
import logging
import os
from dotenv import load_dotenv
import dap.integration.database_errors
from dap.dap_types import Credentials
import asyncio
from dap.api import DAPClient
from dap.integration.database import DatabaseConnection
from dap.replicator.sql import SQLReplicator
import sqlalchemy


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
fileHandler = RotatingFileHandler('./canvas_auto_shifter.log', maxBytes=2000, backupCount=10)
fileHandler.setFormatter(logFormatter)
logger.addHandler(handler)
logger.addHandler(fileHandler)

base_url: str = os.environ["DAP_API_URL"]
client_id: str = os.environ["DAP_CLIENT_ID"]
client_secret: str = os.environ["DAP_CLIENT_SECRET"]
connection_string: str = os.environ["DAP_CONNECTION_STRING"]

credentials = Credentials.create(client_id=client_id, client_secret=client_secret)


async def get_dap_tables(namespace, creds) -> list:
    async with DAPClient(base_url, creds) as session:
        return await session.get_tables(namespace=namespace)


async def init_table_db_sync(table_name, namespace):
    db_connection = DatabaseConnection(connection_string)
    async with DAPClient(base_url, credentials) as session:
        try:
            sql = SQLReplicator(session, db_connection)
            await sql.initialize(namespace, table_name)
        except ValueError as ve:
            if "table already replicated, use `syncdb`" in ve.args:
                logger.info(f"Table {table_name} already initialized")
            else:
                raise ve



async def sync_table_db_sync(table_name, namespace):
    db_connection = DatabaseConnection(connection_string)
    async with DAPClient(base_url, credentials) as session:
        await SQLReplicator(session, db_connection).synchronize(namespace, table_name)


async def main(args):
    eng = sqlalchemy.create_engine(connection_string)
    inspector = sqlalchemy.inspect(eng)
    existing_tables = inspector.get_table_names("canvas")
    # Do web logs
    if "logs" in args:
        log_table_list = await get_dap_tables("canvas_logs", credentials)
        if "init" in args:
            for table_name in log_table_list:
                if table_name in existing_tables:
                    logger.info(f"Skipping initialization of existing log table {table_name}")
                    continue
                logger.info(f'Init Beginning (logs): {table_name}')
                try:
                    await init_table_db_sync(table_name, "canvas_logs", )
                    logger.info(f'Init Completed (logs): {table_name}')
                except dap.integration.database_errors.TableAlreadyExistsError:
                    logger.info(f"Log table {table_name} already initialized")
        if "sync" in args:
            for table_name in log_table_list:
                logger.info(f'Sync Beginning (logs): {table_name}')
                await sync_table_db_sync(table_name, "canvas_logs")
                logger.info(f'Sync Completed (logs): {table_name}')
    if "main" in args:
        table_list = await get_dap_tables("canvas", credentials)
        if "init" in args:
            for table_name in table_list:
                if table_name in existing_tables:
                    logger.info(f"Skipping initialization of existing table {table_name}")
                    continue
                logger.info(f'Init Beginning: {table_name}')
                try:
                    await init_table_db_sync(table_name, "canvas")
                    logger.info(f'Init Completed: {table_name}')
                except dap.integration.database_errors.TableAlreadyExistsError:
                    logger.info(f"Table {table_name} already initialized")
        if "sync" in args:
            for table_name in table_list:
                logger.info(f'Sync Beginning: {table_name}')
                await sync_table_db_sync(table_name, "canvas")
                logger.info(f'Sync Completed: {table_name}')


if __name__ == "__main__":
    if 'seq' in sys.argv:
        asyncio.run(main(sys.argv))
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(sys.argv))
        loop.close()
