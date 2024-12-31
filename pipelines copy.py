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

import pymysql
import traceback
import urllib.parse
from scrapy.utils.project import get_project_settings
import logging

class PyMySQLWriter(object):

    def __init__(self, mysql_url):
        self.mysql_url = mysql_url
        self.conn = None
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(mysql_url=crawler.settings.get('MYSQL_PIPELINE_URL'))

    def open_spider(self, spider):
        conn_kwargs = self.parse_mysql_url(self.mysql_url)
        try:
            self.conn = pymysql.connect(**conn_kwargs, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
            self.logger.info(f"Connected to MySQL database: {conn_kwargs['db']}")
            self._create_table_if_not_exists()
        except pymysql.MySQLError as e:
            self.logger.error(f"Error connecting to MySQL: {e}")
            self.conn = None

    def close_spider(self, spider):
        if self.conn:
            self.conn.close()
            self.logger.info("Closed MySQL connection")

    def process_item(self, item, spider):
        if not self.conn:
            self.logger.error("No database connection available.")
            return item
        try:
            self.do_replace(item)
        except Exception as e:
            self.logger.error(f"Error processing item: {e}\n{traceback.format_exc()}")
        return item

    def _create_table_if_not_exists(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'products'")
            if not cursor.fetchone():
                create_table_sql = """
                CREATE TABLE products (
                    id VARCHAR(255) NOT NULL,
                    title TEXT NOT NULL,
                    sale_price FLOAT NULL,
                    offer_price FLOAT NULL,
                    original_price FLOAT NULL,
                    currency CHAR(3) NOT NULL,
                    star_rating FLOAT NULL,
                    number_reviews INT NULL,
                    total_sales INT NULL,
                    last_scrape_date DATETIME NOT NULL,
                    scrape_status INT NOT NULL,
                    PRIMARY KEY (id)
                )
                """
                cursor.execute(create_table_sql)
                self.logger.info("Created 'products' table")

    def do_replace(self, item):
        sql = """
        REPLACE INTO products (
            id, title, sale_price, offer_price, original_price, currency, star_rating, 
            number_reviews, total_sales, last_scrape_date, scrape_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        args = (
            item.get('id'),
            item.get('title'),
            item.get('sale_price'),
            item.get('offer_price'),
            item.get('original_price'),
            item.get('currency'),
            item.get('star_rating'),
            item.get('number_reviews'),
            item.get('total_sales'),
            item.get('last_scrape_date'),
            item.get('scrape_status')
        )
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
                self.conn.commit()
            self.logger.debug(f"Successfully updated or inserted item with id: {item.get('id')}")
        except pymysql.MySQLError as e:
            self.logger.error(f"Error inserting or updating data in MySQL: {e}")
            self.conn.rollback()

    def parse_mysql_url(self, mysql_url):
        parsed_url = urllib.parse.urlparse(mysql_url)
        conn_kwargs = {
            'host': parsed_url.hostname,
            'user': parsed_url.username,
            'password': parsed_url.password,
            'db': parsed_url.path[1:],
            'port': parsed_url.port if parsed_url.port else 3306,
        }
        return conn_kwargs