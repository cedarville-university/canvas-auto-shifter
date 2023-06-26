import sys
import logging
import os
from dotenv import load_dotenv
import dap.database.database_errors
from dap.dap_types import Credentials
import asyncio
from dap.api import DAPClient
from dap.database.connection import DatabaseConnection
from dap.replicator.sql import SQLReplicator
import sqlalchemy


load_dotenv()
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("dap")
logger.setLevel(logging.INFO)
formatter = logging.Formatter()
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

base_url: str = os.environ["DAP_API_URL"]
client_id: str = os.environ["DAP_CLIENT_ID"]
client_secret: str = os.environ["DAP_CLIENT_SECRET"]
connection_string: str = os.environ["DAP_CONNECTION_STRING"]

credentials = Credentials.create(client_id=client_id, client_secret=client_secret)


async def get_dap_tables() -> list:
    async with DAPClient(base_url, credentials) as session:
        return await session.get_tables(namespace="canvas")


async def init_table_db_sync(table_name):
    async with DatabaseConnection(connection_string).open() as db_connection:
        async with DAPClient(base_url, credentials) as session:
            await SQLReplicator(session, db_connection).initialize("canvas", table_name)

async def sync_table_db_sync(table_name) -> list:
    async with DatabaseConnection(connection_string).open() as db_connection:
        async with DAPClient(base_url, credentials) as session:
            await SQLReplicator(session, db_connection).synchronize("canvas", table_name)


async def main(args):
    table_list = await get_dap_tables()
    eng = sqlalchemy.create_engine(connection_string)
    inspector = sqlalchemy.inspect(eng)
    existing_tables = inspector.get_table_names("canvas")
    if "init" in args:
        for table_name in table_list:
            if table_name in existing_tables:
                logger.info(f"Skipping initialization of existing table {table_name}")
                continue
            logger.info(f'Init Beginning: {table_name}')
            try:
                await init_table_db_sync(table_name)
                logger.info(f'Init Completed: {table_name}')
            except dap.database.database_errors.TableAlreadyExistsError:
                logger.info(f"Table {table_name} already initialized")
    if "sync" in args:
        for table_name in table_list:
            logger.info(f'Sync Beginning: {table_name}')
            await sync_table_db_sync(table_name)
            logger.info(f'Sync Completed: {table_name}')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv))
    loop.close()
