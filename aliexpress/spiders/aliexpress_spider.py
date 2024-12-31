import scrapy
import re
import json
import datetime
import jmespath
import chompjs
from dataclasses import dataclass, asdict
from loguru import logger
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError


def get_number(text):
    if text:
        match = re.search(r"^\d+[+]?", text)
        if match:
            return match.group()
    return None

def safe_float_cast(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

class AliexpressSpider(scrapy.Spider):
    name = 'aliexpress'
    allowed_domains = ['aliexpress.com']

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "COOKIES_ENABLED": True,
        # "CURL_ENABLED": True,
        "DOWNLOADER_MIDDLEWARES": {
              "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 400,           
              "aliexpress.middlewares.RandomUserAgentMiddleware": 500,
              # "aliexpress.middlewares.CurlMiddleware": 600,
        },
        "ITEM_PIPELINES": {
            "aliexpress.pipelines.SQLiteWriter": 400,
        },
        "SQLITE_DATABASE": "products.db",
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 400, 403, 404, 408],
    }

    def __init__(self, query='', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query.strip()
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'referer': 'https://www.aliexpress.com/',
        }

        self.cookies = {
            'aep_usuc_f': 'glo&province=&city=&c_tp=USD&region=US&b_locale=en_US&ae_u_p_s=2',
        }

    def start_requests(self):
        logger.info("Starting the spider...")
        if not self.query:
            logger.error("No query URL provided.")
            return

        if self.query.startswith("https://www.aliexpress.com/w/wholesale"):
            try:
                self.url, self.kwds = self.query.split("wholesale-", 1)
                self.kwds, self.options = self.kwds.split(".html?", 1)
                address = f"{self.url}wholesale-{self.kwds}.html?{self.options}"
                yield scrapy.Request(
                    url=address,
                    headers=self.headers,
                    cookies=self.cookies,
                    callback=self.parse,
                    errback=self.errback_handler,
                )
            except ValueError:
                logger.error("Failed to parse the input URL.")
        else:
            logger.error("Invalid input URL.")

    def parse(self, response):
        body = response.selector
        script = body.xpath('//script[not(@*) and starts-with(normalize-space(text()), "window._dida_config_ =")]/text()').get()
        if not script:
            logger.error("Script block not found in response.")
            return

        try:
            json_extract = chompjs.parse_js_objects(script)
        except Exception as e:
            logger.error(f"Error parsing JS objects: {e}")
            return

        total_results = None
        for extract in json_extract:
            try:
                records = jmespath.search("data.data.root.fields.mods.itemList.content", extract)
                total_results = total_results or jmespath.search("data.data.root.fields.pageInfo.totalResults", extract)
                current_page = jmespath.search("data.data.root.fields.pageInfo.page", extract)

                if records:                   
                    logger.info(f"Processing page {current_page}...")
                    for item in self.extract_fields(records):
                        yield item

                    if current_page < 60:
                        next_page_url = f"{self.url}wholesale-{self.kwds}.html?page={current_page + 1}&{self.options}"
                        yield scrapy.Request(
                            url=next_page_url,
                            headers=self.headers,
                            cookies=self.cookies,
                            callback=self.parse,
                            errback=self.errback_handler,
                        )

            except (jmespath.exceptions.JMESPathTypeError, jmespath.exceptions.ParseError) as e:
                logger.error(f"Error parsing JSON content: {e}")

    def extract_fields(self, records):
        for item in records:
            try:
                product = {
                    "id": item["productId"],
                    "skuId": item["prices"].get("skuId"),
                    "title": item["title"]["displayTitle"],
                    "offer_price": safe_float_cast(item["prices"]["salePrice"]["minPrice"]),
                    "sale_price": safe_float_cast(item["trace"]["utLogMap"]["oip"].split(",", 1)[0]),                   
                    "original_price": safe_float_cast(item["prices"].get("originalPrice",{}).get("minPrice","")),                
                    "currency": item["prices"]["salePrice"]["currencyCode"],
                    "star_rating": safe_float_cast(item.get("evaluation", {}).get("starRating", "")),
                    "number_reviews": None,
                    "total_sales": get_number(item.get("trade", {}).get("tradeDesc", "")),
                    "images": '|'.join('https:'+image['imgUrl'] for image in item["images"][0]),
                    "last_scrape_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "scrape_status": 'S',
                }
                
            except KeyError as e:
                logger.error(f"KeyError extracting fields for item: {e}")
                continue

            headers = self.headers.copy()
            headers.update({
                "accept": "application/json, text/plain, */*",
                "origin": "https://www.aliexpress.com",
                "referer": "https://www.aliexpress.com/",
            })

            yield scrapy.Request(
                url=f"https://feedback.aliexpress.com/pc/searchEvaluation.do?productId={product['id']}",
                headers=headers,
                callback=self.parse_reviews,
                cb_kwargs={"product": product},
            )


    def parse_reviews(self, response, product):
        try:
            json_response = json.loads(response.text)
            result = json_response.get("displayMessage", {}).get("numRatings", "")
            product['number_reviews'] = int(get_number(result)) if get_number(result) else None
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON for product reviews.")
            product.scrape_status = 'F'

        yield product

    def errback_handler(self, failure):
        request = failure.request
        if failure.check(DNSLookupError):
            logger.error(f"DNSLookupError on {request.url}")
        elif failure.check(TimeoutError, TCPTimedOutError):
            logger.error(f"TimeoutError on {request.url}")
        else:
            logger.error(f"Unhandled error on {request.url}: {failure}")
