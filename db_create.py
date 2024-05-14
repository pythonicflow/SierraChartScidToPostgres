import asyncio
import asyncpg

# Establishes a connection to the PostgreSQL database using provided credentials.
# "5432" is the default port for PostgreSQL.
async def db_connect():
    return await asyncpg.connect(
        host="localhost",
        port= "5432", 
        user="your_username",
        password="your_password",
        database="your_database"
    )

# Creates a new PostgreSQL table for storing trading contract data if it does not already exist.
# Default table name is "esm24", but should be modified to match the specific contract symbol you are working with.
# The table schema includes columns for datetime, price, quantity, and market side (bid/ask).
async def create_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS "esm24" (
            scdatetime BIGINT,
            price FLOAT,
            quantity INT,
            side INT
        )
    """)

async def main():
    conn = await db_connect()
    await create_table(conn)
    await conn.close()

asyncio.run(main())