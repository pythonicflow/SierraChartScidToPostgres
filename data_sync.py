import asyncio
import asyncpg
import polars as pl
import numpy as np
import sys
from pathlib import Path
import time
import os
import json

# Establishes a connection to the PostgreSQL database using provided credentials.
async def db_connect():
    return await asyncpg.connect(
        host="localhost",
        user="your_username",
        password="your_password",
        database="your_database"
    )

def get_scid_np(scidFile, offset=0):
    f = Path(scidFile)
    assert f.exists(), "SCID file not found"
    with open(scidFile, 'rb') as file:
        file.seek(0, os.SEEK_END)
        file_size = file.tell()  # Total size of the file
        sciddtype = np.dtype([
            ("scdatetime", "<u8"),
            ("open", "<f4"),
            ("high", "<f4"),
            ("low", "<f4"),
            ("close", "<f4"),
            ("numtrades", "<u4"),
            ("totalvolume", "<u4"),
            ("bidvolume", "<u4"),
            ("askvolume", "<u4"),
        ])
        record_size = sciddtype.itemsize

        # Adjust the offset if not within the file size
        if offset >= file_size:
            offset = file_size - (file_size % record_size)
        elif offset < 56:
            offset = 56  # Skip header assumed to be 56 bytes

        file.seek(offset)
        scid_as_np_array = np.fromfile(file, dtype=sciddtype)
        new_position = file.tell()  # Update the position after reading

    return scid_as_np_array, new_position

# Inserts data into the specified table in the PostgreSQL database. Converts trading volumes to 'quantity' based on whether bid or ask volume is greater.
async def load_data_to_db(conn, df, table_name):
    side_series = pl.col('bidvolume') > 0
    quantity_series = pl.col('bidvolume') > pl.col('askvolume')
    df = df.with_columns([
        pl.when(side_series).then(1).otherwise(0).alias('side'),
        pl.when(quantity_series).then(pl.col('bidvolume')).otherwise(pl.col('askvolume')).alias('quantity'),
        pl.col('close').alias('price')
    ]).select(['scdatetime', 'price', 'quantity', 'side'])
    records = [tuple(row) for row in df.iter_rows()]
    await conn.executemany(f"""
        INSERT INTO {table_name} (scdatetime, price, quantity, side)
        VALUES ($1, $2, $3, $4)
    """, records)

# Coordinates the data processing workflow: connects to the database, reads data from the SCID file, and loads it into the database. Manages checkpoints to handle data continuity.
async def main(table_name, scid_file, initial_load):
    start_time = time.time()
    conn = await db_connect()
    checkpoint_file = Path(f"checkpoint.json")
    if checkpoint_file.exists():
        with open(checkpoint_file, "r") as f:
            checkpoint_data = json.load(f)
            table_data = checkpoint_data.get(table_name, {})
            last_position = table_data.get("last_position", 0)
            initial_load_done = table_data.get("initial_load_done", False)
    else:
        last_position = 0
        initial_load_done = False

    if initial_load and not initial_load_done:
        last_position = 0

    intermediate_np_array, new_position = get_scid_np(scid_file, offset=last_position)
    if new_position > last_position:  # Only update if there's new data
        df_raw = pl.DataFrame(intermediate_np_array)
        await load_data_to_db(conn, df_raw, table_name)
        last_position = new_position  # Updates the last position

        with open(checkpoint_file, "w") as f:
            checkpoint_data = {table_name: {"last_position": last_position, "initial_load_done": True}}
            json.dump(checkpoint_data, f)

    await conn.close()

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

table_name = "esm24"  # Specify the unique table name for your data.
scid_file = "/Volumes/[C] Windows 11/Sierra/Data/ESM24-CME.scid"  # Set the file path to your SCID file.

checkpoint_file = Path(f"checkpoint.json")
initial_load_done = False

# Check if the initial load is done
if checkpoint_file.exists():
    try:
        with open(checkpoint_file, "r") as f:
            checkpoint_data = json.load(f)
            table_data = checkpoint_data.get(table_name, {})
            initial_load_done = table_data.get("initial_load_done", False)
    except json.JSONDecodeError:
        pass
# Run the initial load if it's not done
if not initial_load_done:
    # This is the initial data load
    asyncio.run(main(table_name, scid_file, initial_load=True))

# Continuously update data from SCID file every 'x' seconds. Here, "1" means pause the execution for 1 second between updates.
while True:
    asyncio.run(main(table_name, scid_file, initial_load=False))
    time.sleep(1)  # Pause for 1 second before the next update. Adjust as needed.