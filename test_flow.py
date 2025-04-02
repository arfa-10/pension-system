import time
import json
import redis
import sqlite3
import os
import random
from pension_manager import PensionSystemManager
manager = PensionSystemManager()

try:
    print("\n[Step 1] Creating pensioner in SQL and syncing to Redis...")
    create_data = {
        "FullName": "Selcuk Benchmark",
        "DateOfBirth": "1970-12-12",
        "AadhaarNumber": "100000000001",
        "ContactDetails": "1112223333",
        "PANCard": "PANBENCH01",
        "PPO_Number": "PPOBENCH01",
        "BankID": 101
    }
    result = manager.sql_create("Pensioners", create_data)
    print("SQL Create Result:", result)

    print("\n[Step 2] Reading created record from SQL and measuring time...")
    sql_start = time.time()
    sql_result = manager.sql_read("Pensioners", "AadhaarNumber = ?", ["100000000001"])
    sql_end = time.time()
    print("SQL Read Result:", sql_result)
    sql_time = sql_end - sql_start

    if sql_result and isinstance(sql_result, list):
        pensioner_id = sql_result[0]['PensionerID']
        redis_key = f"pensioner:{str(pensioner_id).zfill(12)}"
        redis_meta_key = f"pensioner:meta:{str(pensioner_id).zfill(12)}"

        print("\n[Step 3] Reading from Redis and measuring time...")
        redis_start = time.time()
        redis_result = manager.redis_read(redis_key)
        redis_end = time.time()
        print("Redis Read Result:", redis_result)
        redis_time = redis_end - redis_start

        # Also fetch metadata
        meta = manager.get_pensioner_meta(pensioner_id)
        print("🔍 Metadata from Redis:", meta)

        print(f"\n⏱️ SQL Read Time: {sql_time:.6f} seconds")
        print(f"⏱️ Redis Read Time: {redis_time:.6f} seconds")

        print("\n[Step 4] Updating FullName in SQL and Redis...")
        update_result = manager.sql_update(
            "Pensioners",
            {"FullName": "Selcuk Updated"},
            "AadhaarNumber = ?",
            ["100000000001"]
        )
        print("SQL Update Result:", update_result)

        print("\n[Step 5] Verifying update in Redis...")
        updated_redis = manager.redis_read(redis_key)
        print("Updated Redis:", updated_redis)

        print("\n[Step 6] Deleting record from SQL and Redis...")
        delete_result = manager.sql_delete("Pensioners", "AadhaarNumber = ?", ["100000000001"])
        print("SQL Delete Result:", delete_result)

        print("\n[Step 7] Checking if Redis key is gone...")
        redis_check = manager.redis_read(redis_key)
        print("Redis After Delete:", redis_check)

        print("\n[Step 8] Checking if metadata is gone (it should still be there)...")
        redis_meta_check = manager.redis_read(redis_meta_key)
        print("Redis Metadata After Delete (optional to clean):", redis_meta_check)
    else:
        print("❌ Failed to read back from SQL. Cannot proceed.")
finally:
    # Step 9: Show metadata for 5 random pensioners
    print("\n[Step 9] Displaying metadata of 5 random pensioners...")
    try:
        pensioners = manager.sql_read("Pensioners")
        random.shuffle(pensioners)
    
        for p in pensioners[:5]:
            pid = p["PensionerID"]
            meta = manager.get_pensioner_meta(pid)
            print(f"PensionerID {pid} Metadata:", json.dumps(meta, indent=2))
    except Exception as e:
        print(f"⚠️ Failed to fetch pensioners metadata: {str(e)}")

    manager.close()
