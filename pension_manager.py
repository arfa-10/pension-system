import redis
import pyodbc
import json
import random
import os
import sys
from datetime import datetime
from prettytable import PrettyTable
from cryptography.fernet import Fernet
import random
import matplotlib.pyplot as plt
import pandas as pd

class PensionSystemManager:
    def __init__(self, redis_json_path='redis_data_synced.json'):
        self.sql_conn = None
        self.redis_client = None
        self.redis_json_path = redis_json_path
        self.initialize_databases()
        self.secret_key = os.environ.get("REDIS_SECRET_KEY") or Fernet.generate_key()
        self.fernet = Fernet(self.secret_key)
        print(f"🔑 Redis encryption key loaded.")
        #self.load_redis_data()


    def initialize_databases(self):
        """Initialize connections to both databases"""
        # SQL Server Connection
        try:
            server = 'tcp:mcruebs04.isad.isadroot.ex.ac.uk'
            database = 'BEMM459_GroupF'
            username = 'GroupF'
            password = 'LefT464+Ez'
            
            connection_string = (
                'DRIVER={SQL Server};SERVER=' + server + 
                ';DATABASE=' + database + 
                ';UID=' + username + 
                ';PWD=' + password +
                ';TrustServerCertificate=yes;Encrypt=no;'
            )
            
            self.sql_conn = pyodbc.connect(connection_string)
            print("✅ SQL Server connection successful")
        except Exception as e:
            print(f"❌ SQL Server connection failed: {str(e)}")
            sys.exit(1)

        # Redis Connection
        try:
            self.redis_client = redis.Redis(
                host='redis-19057.c278.us-east-1-4.ec2.redns.redis-cloud.com',
                port=19057,
                password='nxgpjlbCPGNHPqYMDAKuhdK4q5yJyXiJ',
                decode_responses=True,
                ssl=False  # ✅ Required for Redis Cloud
            )

            self.redis_client.ping()
            print("✅ Redis connection successful")
        except Exception as e:
            print(f"❌ Redis connection failed: {str(e)}")
            sys.exit(1)

    def load_redis_data(self):
        if not os.path.exists(self.redis_json_path):
            print(f"⚠️ No Redis JSON file found at {self.redis_json_path}")
            return
        try:
            # 🧹 Clear old data first (optional, but prevents re-uploading same records)
        #     print("♻️ Deleting existing Redis pensioner and bank keys for demo purposes..")
        #     for key in self.redis_client.scan_iter("pensioner:*"):
        #         self.redis_client.delete(key)
        #     for key in self.redis_client.scan_iter("bank:*"):
        #         self.redis_client.delete(key)
        # #try:
            with open(self.redis_json_path) as f:
                data = json.load(f)
    
            for data_type in ['banks', 'pensioners', 'links', 'transactions']:
                items = data.get(data_type, [])
                if not isinstance(items, list):
                    continue
                for item in items:
                    if isinstance(item, dict) and 'key' in item and 'data' in item:
                        key = item['key']
                        data_item = item['data']
                        if self.redis_client.exists(key):
                            continue  # ✅ Skip if key already in Redis

                        # Only encrypt if relevant fields are present
                        if key.startswith("pensioner:") and not key.startswith("pensioner:meta"):
                            if all(k in data_item for k in ['full_name', 'aadhaar_number', 'contact']):
                                try:
                                    data_item["full_name"] = self.encrypt(data_item["full_name"])
                                    data_item["aadhaar_number"] = self.encrypt(data_item["aadhaar_number"])
                                    if isinstance(data_item["contact"], dict) and "phone" in data_item["contact"]:
                                        data_item["contact"]["phone"] = self.encrypt(data_item["contact"]["phone"])
                                except KeyError:
                                    pass
    
                        self.redis_client.json().set(key, '$', data_item)
    
                        pensioner_id = data_item.get('sql_reference', {}).get('id')
                        if pensioner_id is not None:
                            meta_key = f"pensioner:meta:{str(pensioner_id).zfill(12)}"
                            if not self.redis_client.exists(meta_key):
                                self.set_pensioner_meta(pensioner_id, {
                                    "device": random.choice(["iOS", "Android", "Windows"]),
                                    "onboarding_complete": random.choice([True, False]),
                                    "preferred_language": random.choice(["en", "tr", "de"]),
                                    "login_count": random.randint(1, 50),
                                    "last_login": datetime.now().isoformat()
                                })
    
            print(f"✅ Loaded Redis data from {self.redis_json_path}")
        except Exception as e:
            print(f"❌ Error loading Redis data: {str(e)}")

    def encrypt(self, text):
        return self.fernet.encrypt(text.encode()).decode()

    def decrypt(self, encrypted_text):
        return self.fernet.decrypt(encrypted_text.encode()).decode()
    def decrypt_pensioner_data(self, data):
        try:
            data['full_name'] = self.decrypt(data['full_name'])
            data['aadhaar_number'] = self.decrypt(data['aadhaar_number'])
            data['contact']['phone'] = self.decrypt(data['contact']['phone'])
        except:
            pass
        return data
    # ========== SQL CRUD OPERATIONS ==========
    

    def sql_create(self, table, data):
        """Insert data into SQL table and sync to Redis if supported."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        result = self._execute_sql(query, tuple(data.values()))
    
        redis_key = None
        new_id = None
    
        if 'error' not in result:
            try:
                if table.lower() == 'pensioners':
                    query = f"SELECT TOP 1 * FROM Pensioners ORDER BY PensionerID DESC"
                    latest = self._execute_sql(query)
                    if isinstance(latest, list) and latest:
                        new_id = latest[0]['PensionerID']
                        redis_key = f"pensioner:{str(new_id).zfill(12)}"
                        redis_data = {
                            "full_name": latest[0]['FullName'],
                            "aadhaar_number": latest[0]['AadhaarNumber'],
                            "contact": {"phone": latest[0]['ContactDetails']},
                            "sql_reference": {"table": "Pensioners", "id": new_id}
                        }
                        self.redis_create(redis_key, {
                        "full_name": self.encrypt(latest[0]['FullName']),
                        "aadhaar_number": self.encrypt(latest[0]['AadhaarNumber']),
                        "contact": {"phone": self.encrypt(latest[0]['ContactDetails'])},
                        "sql_reference": {"table": "Pensioners", "id": new_id}  # ✅ fix here
                    })


            except Exception as e:
                print(f"⚠️ Failed to create Redis mirror: {str(e)}")
    
        # Include the new ID and Redis key in the return
        if not isinstance(result, dict):
            result = {"rows_affected": 0}
        
        result['new_id'] = new_id
        result["redis_key"] = redis_key

        return result


    def sql_update(self, table, data, where, params=None):
        """Update SQL table and sync to Redis if supported."""
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        full_params = tuple(data.values()) + tuple(params if params else ())

        result = self._execute_sql(query, full_params)

        if 'error' not in result:
            if table.lower() == 'pensioners':
                pensioners = self.sql_read(table, where, params)
                for record in pensioners:
                    redis_key = f"pensioner:{int(record['PensionerID']):012d}"
                    redis_data = {
                        "full_name": record.get("FullName"),
                        "aadhaar_number": record.get("AadhaarNumber"),
                        "contact": {"phone": record.get("ContactDetails")},
                        "sql_reference": {"table": "Pensioners", "id": record['PensionerID']}
                    }
                    self.redis_update(redis_key, redis_data)

            elif table.lower() == 'banks':
                banks = self.sql_read(table, where, params)
                for record in banks:
                    redis_key = f"bank:BK{int(record['BankID']):04d}"
                    redis_data = {
                        "legal_name": record.get("BankName"),
                        "ifsc_code": record.get("IFSCCode"),
                        "branch": record.get("BranchAddress")
                    }
                    self.redis_update(redis_key, redis_data)

        return result

    def sql_delete(self, table, where, params=None):
        if table.lower() == 'pensioners':
            try:
                self.sql_cursor = self.sql_conn.cursor()
                self.sql_cursor.execute(f"SELECT PensionerID FROM {table} WHERE {where}", params)
                rows = self.sql_cursor.fetchall()
                for row in rows:
                    pensioner_id = row[0]
                    self.redis_delete(f"pensioner:{str(pensioner_id).zfill(12)}")
                    self.redis_delete(f"pensioner:meta:{str(pensioner_id).zfill(12)}")
            except Exception as e:
                print(f"⚠️ Could not sync delete to Redis: {str(e)}")
        query = f"DELETE FROM {table} WHERE {where}"
        result = self._execute_sql(query, params)
        print(f"🧹 Rows deleted from {table}: {result.get('rows_affected', 0)}")

        self.sql_conn.commit()
        return result

   

    def sql_read(self, table, where=None, params=None):
        """Read data from SQL table"""
        query = f"SELECT * FROM {table}"
        if where:
            query += f" WHERE {where}"
        return self._execute_sql(query, params)


    def set_pensioner_meta(self, pensioner_id, meta_dict):
        redis_key = f"pensioner:meta:{str(pensioner_id).zfill(12)}"
        try:
            self.redis_client.json().set(redis_key, '$', meta_dict)
            print(f"✅ Set dynamic metadata for Pensioner {pensioner_id}")
        except Exception as e:
            print(f"❌ Failed to set meta: {str(e)}")

            
    def get_pensioner_meta(self, pensioner_id):
        redis_key = f"pensioner:meta:{str(pensioner_id).zfill(12)}"
        try:
            return self.redis_client.json().get(redis_key)
        except Exception as e:
            print(f"❌ Failed to retrieve meta: {str(e)}")
            return None

    def populate_metadata_for_existing_pensioners(self):
        result = self._execute_sql("SELECT PensionerID FROM Pensioners")
        if isinstance(result, list):
            for row in result:
                pensioner_id = row['PensionerID']
                meta_key = f"pensioner:meta:{str(pensioner_id).zfill(12)}"
                if not self.redis_client.exists(meta_key):
                    self.set_pensioner_meta(pensioner_id, {
                        "device": "Unknown",
                        "onboarding_complete": False,
                        "preferred_language": "en"
                    })
    def _execute_sql(self, query, params=None):
        """Execute SQL query and return results"""
        try:
            cursor = self.sql_conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            else:
                self.sql_conn.commit()
                return {"rows_affected": cursor.rowcount}
        except Exception as e:
            self.sql_conn.rollback()
            return {"error": str(e)}

    # ========== REDIS CRUD OPERATIONS ==========
    def redis_create(self, key, value, expire=None):
        try:
            if isinstance(value, (dict, list)):
                self.redis_client.json().set(key, '$', value)
            else:
                self.redis_client.set(key, value)
            if expire:
                self.redis_client.expire(key, expire)
            return {"success": True, "key": key}
        except Exception as e:
            return {"error": str(e)}

    def redis_update(self, key, value):
        return self.redis_create(key, value)

    def redis_delete(self, key):
        try:
            deleted = self.redis_client.delete(key)
            return {"deleted": deleted}
        except Exception as e:
            return {"error": str(e)}




    
    def redis_create(self, key, value, expire=None):
        """Create/update Redis key"""
        try:
            if isinstance(value, (dict, list)):
                self.redis_client.json().set(key, '$', value)
            else:
                self.redis_client.set(key, value)
            
            if expire:
                self.redis_client.expire(key, expire)
            return {"success": True, "key": key}
        except Exception as e:
            return {"error": str(e)}

    def redis_read(self, key):
        try:
            try:
                data = self.redis_client.json().get(key)
                if data is not None and key.startswith("pensioner:") and not key.startswith("pensioner:meta"):
                    return self.decrypt_pensioner_data(data)
                return data
            except:
                raw = self.redis_client.get(key)
                return self.decrypt(raw.decode()) if raw else None
        except Exception as e:
            return {"error": str(e)}


    def redis_update(self, key, value):
        """Update Redis key (same as create)"""
        return self.redis_create(key, value)

    def redis_delete(self, key):
        """Delete Redis key"""
        try:
            deleted = self.redis_client.delete(key)
            return {"deleted": deleted}
        except Exception as e:
            return {"error": str(e)}

    def redis_search(self, pattern):
        """Search Redis keys"""
        try:
            return self.redis_client.keys(pattern)
        except Exception as e:
            return {"error": str(e)}

    # ========== DISPLAY METHODS ==========
    def display_sql_table(self, table_name):
        """Display SQL table contents in formatted table"""
        try:
            result = self.sql_read(table_name)
            if isinstance(result, list) and len(result) > 0:
                table = PrettyTable()
                table.field_names = result[0].keys()
                
                for row in result:
                    table.add_row(row.values())
                
                print(f"\n📊 SQL Table: {table_name}")
                print(table)
                print(f"Total rows: {len(result)}")
            else:
                print(f"Table '{table_name}' is empty or doesn't exist")
        except Exception as e:
            print(f"❌ Error displaying table: {str(e)}")

    def display_redis_data(self, pattern='*'):
        """Display Redis data matching pattern in formatted tables"""
        try:
            keys = self.redis_search(pattern)
            if not keys:
                print(f"No Redis keys found matching pattern: {pattern}")
                return
            
            print(f"\n🔍 Redis Data (pattern: {pattern})")
            
            for key in keys:
                try:
                    data = self.redis_read(key)
                    if data is None:
                        continue
                        
                    table = PrettyTable()
                    table.title = f"Key: {key}"
                    
                    if isinstance(data, dict):
                        table.field_names = ["Field", "Value"]
                        for k, v in data.items():
                            if isinstance(v, (list, dict)):
                                v = json.dumps(v, indent=2)
                            table.add_row([k, v])
                    else:
                        table.field_names = ["Value"]
                        table.add_row([data])
                    
                    print(table)
                    print("-" * 50)
                except Exception as e:
                    print(f"⚠️ Error displaying key {key}: {str(e)}")
                
            
            print(f"Total keys displayed: {len(keys)}")
        except Exception as e:
            print(f"❌ Error searching Redis: {str(e)}")
    
    # ========== CUSTOM QUERY METHODS ==========
    def sql_custom_query(self, query, params=None):
        """Execute custom SQL query"""
        return self._execute_sql(query, params)
    def delete_pensioner_cascade(self, pensioner_id):
        try:
            # Step 1: Find dependent tables dynamically
            query = """
            SELECT OBJECT_NAME(fk.parent_object_id) AS TableName
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
            JOIN sys.tables t ON fkc.parent_object_id = t.object_id
            WHERE fk.referenced_object_id = OBJECT_ID('Pensioners') AND c.name = 'PensionerID'
            """
            tables = self._execute_sql(query)
    
            # Step 2: Delete from dependent tables
            for t in tables:
                table_name = t['TableName']
                print(f"🧹 Deleting from {table_name} where PensionerID = {pensioner_id}")
                self._execute_sql(f"DELETE FROM {table_name} WHERE PensionerID = ?", [pensioner_id])
    
            # Step 3: Delete from Pensioners
            self._execute_sql("DELETE FROM Pensioners WHERE PensionerID = ?", [pensioner_id])
            self.sql_conn.commit()
    
            # Step 4: Delete from Redis
            self.redis_delete(f"pensioner:{str(pensioner_id).zfill(12)}")
            self.redis_delete(f"pensioner:meta:{str(pensioner_id).zfill(12)}")
            print(f"✅ Pensioner {pensioner_id} and related records deleted.")
        except Exception as e:
            self.sql_conn.rollback()
            print(f"❌ Failed cascading delete: {str(e)}")
    
    
    
    

    def redis_custom_command(self, *args):
        """Execute custom Redis command"""
        try:
            result = self.redis_client.execute_command(*args)
            return {"result": result}
        except Exception as e:
            return {"error": str(e)}

    # ========== MENU SYSTEM ==========
    def display_menu(self):
        """Display interactive menu"""
        while True:
            print("\n===== Pension System Manager =====")
            print(f"Current Redis JSON: {self.redis_json_path}")
            print("1. SQL CRUD Operations")
            print("2. Redis CRUD Operations")
            print("3. Display SQL Tables")
            print("4. Display Redis Data")
            print("5. Run Custom Query/Command")
            print("6. Change Redis JSON Path")
            print("7. Exit")
            print("8. Visualize Pensioner Metadata")
            
                        
            choice = input("Select option (1-8): ")
            
            if choice == '1':
                self.sql_crud_menu()
            elif choice == '2':
                self.redis_crud_menu()
            elif choice == '3':
                self.display_sql_tables_menu()
            elif choice == '4':
                self.display_redis_data_menu()
            elif choice == '5':
                self.custom_query_menu()
            elif choice == '6':
                self.change_json_path()
            elif choice == '7':
                break
            elif choice == '8':
                self.visualize_pensioner_metadata()
            else:
                print("Invalid choice, please try again")

    def change_json_path(self):
        """Change the Redis JSON file path"""
        new_path = input("Enter new path to Redis JSON file: ")
        if os.path.exists(new_path):
            self.redis_json_path = new_path
            print(f"Redis JSON path updated to: {new_path}")
            # Reload data with new path
            self.load_redis_data()
        else:
            print(f"File not found at: {new_path}")

    def sql_crud_menu(self):
        """SQL CRUD operations menu"""
        while True:
            print("\n===== SQL CRUD Operations =====")
            print("1. Create Record")
            print("2. Read Records")
            print("3. Update Record")
            print("4. Delete Record")
            print("5. Back to Main Menu")
            
            choice = input("Select operation (1-5): ")
            
            if choice == '1':
                table = input("Enter table name: ")
                columns = input("Enter column names (comma separated): ").split(',')
                values = input("Enter values (comma separated): ").split(',')
                data = dict(zip([col.strip() for col in columns], [val.strip() for val in values]))
                result = self.sql_create(table, data)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '2':
                table = input("Enter table name: ")
                where = input("Enter WHERE clause (leave empty for all): ")
                params = None
                if where:
                    params = input("Enter parameters (comma separated): ").split(',')
                    params = [p.strip() for p in params]
                result = self.sql_read(table, where, params)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '3':
                table = input("Enter table name: ")
                columns = input("Enter columns to update (comma separated): ").split(',')
                values = input("Enter new values (comma separated): ").split(',')
                data = dict(zip([col.strip() for col in columns], [val.strip() for val in values]))
                where = input("Enter WHERE clause: ")
                params = input("Enter WHERE parameters (comma separated): ").split(',')
                params = [p.strip() for p in params]
                result = self.sql_update(table, data, where, params)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '4':  # Delete Record
                table = input("Enter table name: ").strip()
                where = input("Enter WHERE clause: ").strip()
                params = input("Enter parameters (comma separated): ").split(',')
                params = [p.strip() for p in params]
            
                if table.lower() == "pensioners":
                    # Try extracting PensionerID for cascade delete
                    try:
                        match = self.sql_read(table, where, params)
                        if match and isinstance(match, list) and 'PensionerID' in match[0]:
                            pensioner_id = match[0]['PensionerID']
                            confirm = input(f"⚠️ This will delete PensionerID {pensioner_id} and all related transactions. Continue? (y/n): ")
                            if confirm.lower() == 'y':
                                self.delete_pensioner_cascade(pensioner_id)
                            else:
                                print("❌ Delete cancelled.")
                        else:
                            print("❌ No matching pensioner found or PensionerID missing.")
                    except Exception as e:
                        print(f"❌ Error during cascading delete: {str(e)}")
                else:
                    result = self.sql_delete(table, where, params)
                    print("Result:", json.dumps(result, indent=2))


    def redis_crud_menu(self):
        """Redis CRUD operations menu"""
        while True:
            print("\n===== Redis CRUD Operations =====")
            print("1. Create/Set Key")
            print("2. Get Key")
            print("3. Update Key")
            print("4. Delete Key")
            print("5. Search Keys")
            print("6. Back to Main Menu")
            
            choice = input("Select operation (1-6): ")
            
            if choice == '1':
                key = input("Enter key: ")
                value = input("Enter value (JSON for complex data): ")
                try:
                    value = json.loads(value) if value.startswith('{') or value.startswith('[') else value
                except:
                    pass
                expire = input("Expire in seconds (leave empty for none): ")
                expire = int(expire) if expire else None
                result = self.redis_create(key, value, expire)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '2':
                key = input("Enter key: ")
                result = self.redis_read(key)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '3':
                key = input("Enter key: ")
                value = input("Enter new value (JSON for complex data): ")
                try:
                    value = json.loads(value) if value.startswith('{') or value.startswith('[') else value
                except:
                    pass
                result = self.redis_update(key, value)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '4':
                key = input("Enter key: ")
                result = self.redis_delete(key)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '5':
                pattern = input("Enter key pattern (e.g., 'user:*'): ")
                result = self.redis_search(pattern)
                print("Result:", json.dumps(result, indent=2))
                
            elif choice == '6':
                break
            else:
                print("Invalid choice, please try again")

    def display_sql_tables_menu(self):
        """Menu for displaying SQL tables"""
        try:
            cursor = self.sql_conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
            """)
            tables = [row.table_name for row in cursor.fetchall()]
            
            print("\n===== Available SQL Tables =====")
            for i, table in enumerate(tables, 1):
                print(f"{i}. {table}")
            print(f"{len(tables)+1}. Back to Main Menu")
            
            choice = input(f"Select table to display (1-{len(tables)+1}): ")
            
            if choice.isdigit() and 1 <= int(choice) <= len(tables):
                self.display_sql_table(tables[int(choice)-1])
            elif choice == str(len(tables)+1):
                return
            else:
                print("Invalid choice")
        except Exception as e:
            print(f"Error getting tables: {str(e)}")

    def display_redis_data_menu(self):
        """Menu for displaying Redis data"""
        print("\n===== Redis Data Display Options =====")
        print("1. Display all data")
        print("2. Display banks")
        print("3. Display pensioners")
        print("4. Display transactions")
        print("5. Custom pattern search")
        print("6F. Back to Main Menu")
        
        choice = input("Select option (1-6): ")
        
        if choice == '1':
            self.display_redis_data('*')
        elif choice == '2':
            self.display_redis_data('bank:*')
        elif choice == '3':
            self.display_redis_data('pensioner:*')
        elif choice == '4':
            self.display_redis_data('txn:*')
        elif choice == '5':
            pattern = input("Enter Redis key pattern (e.g., 'user:*'): ")
            self.display_redis_data(pattern)
        elif choice == '6':
            return
        else:
            print("Invalid choice")

    def custom_query_menu(self):
        """Menu for custom queries"""
        print("\n===== Custom Query/Command =====")
        print("1. SQL Query")
        print("2. Redis Command")
        print("3. Back to Main Menu")
        
        choice = input("Select option (1-3): ")
        
        if choice == '1':
            query = input("Enter SQL query: ")
            params = input("Enter parameters (comma separated, leave empty if none): ")
            params = [p.strip() for p in params.split(',')] if params else None
            result = self.sql_custom_query(query, params)
            print("Result:", json.dumps(result, indent=2))
            
        elif choice == '2':
            command = input("Enter Redis command (e.g., 'FT.SEARCH idx:pensioners *'): ")
            parts = command.split()
            result = self.redis_custom_command(*parts)
            print("Result:", json.dumps(result, indent=2))
            
        elif choice == '3':
            return
        else:
            print("Invalid choice")
    def visualize_pensioner_metadata(self):
        """Plot bar charts for login count and preferred language distribution."""
        keys = self.redis_search('pensioner:meta:*')
        metadata = []
        for key in keys:
            data = self.redis_read(key)
            if isinstance(data, dict):
                metadata.append(data)

        if not metadata:
            print("No metadata found to visualize.")
            return

        df = pd.DataFrame(metadata)

        plt.figure(figsize=(10, 5))
        df['preferred_language'].value_counts().plot(kind='bar')
        plt.title('Preferred Language Distribution')
        plt.xlabel('Language')
        plt.ylabel('Count')
        plt.tight_layout()
        plt.show()

        plt.figure(figsize=(10, 5))
        df['login_count'].plot(kind='hist', bins=10)
        plt.title('Login Count Distribution')
        plt.xlabel('Login Count')
        plt.tight_layout()
        plt.show()

    def close(self):
        """Clean up resources"""
        if self.sql_conn:
            self.sql_conn.close()
        if self.redis_client:
            self.redis_client.close()
        print("Database connections closed")

# Main execution
if __name__ == "__main__":
    # Check for command line argument for JSON path
    json_path = 'redis_data_synced.json'
    if len(sys.argv) > 1:
        json_path = sys.argv[1]
        print(f"Using JSON file from command line: {json_path}")
    
    manager = PensionSystemManager(json_path)
    try:
        manager.display_menu()
    finally:
        manager.close()