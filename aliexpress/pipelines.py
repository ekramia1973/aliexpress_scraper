# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
# from scrapy.exceptions import DropItem

class AliexpressPipeline:
    def process_item(self, item, spider):
        return item

import sqlite3
import traceback
import logging
from typing import Any, Dict, List, Optional
from scrapy.exceptions import NotConfigured

class SQLiteWriter:
    def __init__(self, database_name: str, table_name: str = "products") -> None:
        self.database_name = database_name
        self.table_name = table_name
        self.conn: Optional[sqlite3.Connection] = None
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler) -> "SQLiteWriter":
        database_name = crawler.settings.get('SQLITE_DATABASE')
        if not database_name:
            raise NotConfigured("SQLITE_DATABASE setting is required")
        return cls(database_name=database_name)

    def open_spider(self, spider) -> None:
        try:
            self.conn = sqlite3.connect(self.database_name)
            self.conn.text_factory = lambda x: x.decode("utf-8", errors="ignore")
            self.conn.row_factory = sqlite3.Row
            self.logger.info(f"Connected to SQLite database: {self.database_name}")
            self._create_table_if_not_exists()
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to SQLite: {e}")
            raise

    def close_spider(self, spider) -> None:
        if self.conn:
            self.conn.close()
            self.logger.info("Closed SQLite connection")

    def process_item(self, item: Dict[str, Any], spider) -> Dict[str, Any]:
        if not self.conn:
            self.logger.error("No database connection available.")
            return item
        try:
            self._do_update_or_insert(item)
        except Exception as e:
            self.logger.error(
                f"Error processing item with id {item.get('id', 'UNKNOWN')}: {e}\n{traceback.format_exc()}"
            )
        return item

    def _create_table_if_not_exists(self) -> None:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id TEXT NOT NULL PRIMARY KEY,
            skuid TEXT Not NULL,
            title TEXT NOT NULL,
            main_image TEXT,
            url TEXT NOT NULL,
            sale_price REAL,
            original_price REAL,
            discount REAL,
            currency CHAR(3) NOT NULL,
            trade_count INTEGER,
            store_name TEXT,
            store_url TEXT,
            star_rating REAL,
            number_reviews INTEGER,
            total_sales TEXT,
            images TEXT,
            last_scrape_date TEXT NOT NULL,
            scrape_status CHAR NOT NULL
        )
        """
        try:
            with self.conn:
                self.conn.execute(create_table_sql)
            self.logger.debug(f"Created '{self.table_name}' table")
        except sqlite3.Error as e:
            self.logger.error(f"Error creating table: {e}")
            raise

    def _do_update_or_insert(self, item: Dict[str, Any]) -> None:
        keys = item.keys()
        columns = ", ".join(keys)
        placeholders = ", ".join("?" for _ in keys)
        args = tuple(item[key] for key in keys)

        # Check if the record already exists
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM {self.table_name} WHERE id = ?", (item["id"],))
            existing_record = cursor.fetchone()
        except sqlite3.Error as e:
            self.logger.error(f"Error checking for existing record with id: {item['id']} - {e}")
            raise

        if existing_record:
            # Prepare the UPDATE clause for existing records
            update_columns = []
            update_args = []

            for key in keys:
                if key == "last_scrape_date":
                    update_columns.append(f"{key} = ?")
                    update_args.append(item[key])  # Always update last_scrape_date
                else:
                    # Only update if the value differs
                    existing_value = existing_record[key]  # Access the existing value by key
                    if item[key] != existing_value:
                        update_columns.append(f"{key} = ?")
                        update_args.append(item[key])

            if update_columns:
                # Update only if there's something to update
                update_clause = ", ".join(update_columns)
                update_sql = f"UPDATE {self.table_name} SET {update_clause} WHERE id = ?"
                update_args.append(item["id"])  # Add the id for the WHERE clause
                try:
                    with self.conn:
                        self.conn.execute(update_sql, update_args)
                except sqlite3.Error as e:
                    self.logger.error(f"Error updating record with id: {item['id']} - {e}")
                    raise
            
        else:
            # Insert a new record if it doesn't exist
            insert_sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            try:
                with self.conn:
                    self.conn.execute(insert_sql, args)
            except sqlite3.Error as e:
                self.logger.error(f"Error inserting new record with id: {item['id']} - {e}")
                raise


import json
import os

class JsonWriter:
    def __init__(self, filename="products.json"):
        self.filename = filename
        self.data_cache = []
        self.batch_size = 10  # Adjust batch size as needed
        # Ensure the file exists
        if not os.path.exists(self.filename):
            with open(self.filename, "w") as file:
                json.dump([], file)
    
    def process_item(self, item, spider):
        try:
            # Convert images field from JSON string to list if necessary
            if isinstance(item.get("images"), str):
                try:
                    item["images"] = json.loads(item["images"])
                except json.JSONDecodeError:
                    spider.logger.error("Error decoding images field to list")
                    item["images"] = []
            
            # Read existing data
            with open(self.filename, "r") as file:
                existing_data = json.load(file)
            
            # Check if item already exists
            product_index = next((i for i, prod in enumerate(existing_data) if prod.get("id") == item["id"]), None)
            
            if product_index is not None:
                # Check if any field has changed
                existing_product = existing_data[product_index]
                if any(existing_product.get(k) != item[k] for k in item):
                    existing_data[product_index] = item  # Update record
            else:
                self.data_cache.append(item)  # Add new record to cache
            
            # Save when cache reaches batch size
            if len(self.data_cache) >= self.batch_size:
                self._flush_data(existing_data)
        except Exception as e:
            spider.logger.error(f"Error processing item: {e}")
        return item
    
    def _flush_data(self, existing_data):
        try:
            # Append cached data
            existing_data.extend(self.data_cache)
            
            # Write back to file
            with open(self.filename, "w") as file:
                json.dump(existing_data, file, indent=4)
            
            # Clear cache
            self.data_cache = []
        except Exception as e:
            print(f"Error saving batch data: {e}")
