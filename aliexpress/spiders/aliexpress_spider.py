import scrapy
import datetime
from scrapy.utils import spider
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError
import json
import random
import re
from urllib.parse import urlparse
import jmespath
from w3lib.html import remove_tags, replace_escape_chars
from html import unescape

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

def cleanup(input_text):
    return unescape(remove_tags(replace_escape_chars(input_text)))

def parse_reviews(response, product):
    try:
        json_response = json.loads(response.text)
        display_message = (json_response.get("displayMessage", {}))
        num_ratings = display_message.get("numRatings", 0)
        product['number_reviews'] = get_number(num_ratings) if get_number(num_ratings) else None
    except json.JSONDecodeError:
        spider.logger.error(f"Failed to decode JSON for product {product['id']} reviews.")
        product['scrape_status'] = 'failed'

    yield product


def errback_handler(failure):
    request = failure.request
    if failure.check(DNSLookupError):
        spider.logger.error(f"DNSLookupError on {request.url}")
    elif failure.check(TimeoutError, TCPTimedOutError):
        spider.logger.error(f"TimeoutError on {request.url}")
    else:
        spider.logger.error(f"Unhandled error on {request.url}: {failure}")


class AliexpressSpider(scrapy.Spider):
    name = 'aliexpress'

    custom_settings = {
        "USER-AGENT": None,
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": True,
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,

        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },

        "ITEM_PIPELINES": {
            "aliexpress.pipelines.SQLiteWriter": 400,
            "aliexpress.pipelines.JsonWriter":401,
        },
        "SQLITE_DATABASE": "products.db",

        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "FEED_EXPORT_ENCODING": "utf-8",

        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 400, 403, 404, 408],
    }

    def __init__(self, query='', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query.strip()
        self.parsed_query = urlparse(self.query)
        self.prefix, self.domain = self.parsed_query.netloc.split('.', 1)
        self.browsers_list = [
                                 "chrome99", "chrome100", "chrome101", "chrome104", "chrome107", "chrome110",
                                 "chrome116", "chrome119"
                             ] * 15 + ["chrome120", "chrome123", "chrome124", "chrome131"] * 65 + ["chrome99_android",
                                                                                                   "chrome131_android"] + [
                                 "edge99", "edge101"
                             ] * 11 + ["safari15_3", "safari15_5", "safari17_0", "safari18_0"] * 5 + ["safari17_2_ios",
                                                                                                      "safari18_0_ios"] + [
                                 "firefox133"
                             ] * 4

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'referer': f'https://{self.parsed_query.netloc}/',
        }

        self.cookies = {
            'aep_usuc_f': 'glo&province=&city=&c_tp=USD&region=US&b_locale=en_US&ae_u_p_s=2',
        }

    def start_requests(self):
        spider.logger.info("Starting the spider...")
        if not self.query:
            spider.logger.error("No query URL provided.")
            return

        if "www.aliexpress" in self.parsed_query.netloc and self.parsed_query.path.startswith("/w/wholesale"):
            try:
                yield scrapy.Request(
                    url=self.query,
                    headers=self.headers,
                    cookies=self.cookies,
                    callback=self.parse,
                    errback=errback_handler,
                    meta={"impersonate": random.choice(self.browsers_list)},
                )
            except ValueError:
                spider.logger.error("Failed to parse the input URL.")
        else:
            spider.logger.error("Invalid input URL.")

    def parse(self, response):
        body = response.selector
        script = body.xpath(
            '//script[not(@*) and starts-with(normalize-space(text()), "window._dida_config_ =")]/text()').get()
        if not script:
            spider.logger.error("Script block not found in response.")
            return
        pattern = r'(\"data\":\s*{.*})'
        match = re.search(pattern, script)
        if not match:
            print("no matches found!")
            return
        try:
            # json_extract = chompjs.parse_js_objects(script)
            extract = "{" + match.group(1).rsplit('}', 1)[0].strip()
            extract = json.loads(extract)
        except Exception as e:
            spider.logger.error(f"Error parsing JS objects: {e}")
            return

        try:
            records = jmespath.search("data.root.fields.mods.itemList.content", extract)
            current_page = jmespath.search("data.root.fields.pageInfo.page", extract)

            if records:
                spider.logger.info(f"Processing page {current_page}...")
                for item in self.extract_fields(records, response):
                    yield item

                headers = response.headers

                if current_page < 60:
                    next_page_url = ''
                    if 'page=' in response.url:
                        next_page_url = re.sub(r'page=(\d+)', f'page={current_page + 1}', response.url)
                    else:
                        next_page_url = f"{response.url}&page={current_page + 1}" if '?' in response.url else f"{response.url}?page={current_page + 1}"

                    yield scrapy.Request(
                        url=next_page_url,
                        headers=headers,
                        cookies=self.cookies,
                        callback=self.parse,
                        errback=errback_handler,
                        meta={"impersonate": random.choice(self.browsers_list)},
                    )

        except (jmespath.exceptions.JMESPathTypeError, jmespath.exceptions.ParseError) as e:
            spider.logger.error(f"Error parsing JSON content: {e}")

    def extract_fields(self, records, response):
        for item in records:
            try:
                product = {
                    "id": item["productId"],
                    "skuId": item["prices"].get("skuId"),
                    "title": cleanup(item["title"]["displayTitle"]),
                    "main_image": f'https:{item["image"]["imgUrl"]}',
                    "url": f"{self.parsed_query.scheme}://{self.parsed_query.netloc}/item/{item["productId"]}",
                    "sale_price": safe_float_cast(item["prices"]["salePrice"]["minPrice"]),
                    "original_price": safe_float_cast(item["prices"].get("originalPrice", {}).get("minPrice", "")),
                    "discount": safe_float_cast(item["prices"]["salePrice"].get("discount", 0)),
                    "currency": item["prices"]["salePrice"]["currencyCode"],
                    "trade_count": get_number(item.get("trade", {}).get("realTradeCount", 0)),
                    "store_name": item.get("store", {}).get("storeName", ""),
                    "store_url": 'https:' + (url_:=item.get("store", {}).get("storeUrl", "")) if url_ else '',
                    "star_rating": safe_float_cast(item.get("evaluation", {}).get("starRating", "")),
                    "number_reviews": None,
                    "total_sales": get_number(item.get("trade", {}).get("tradeDesc", "")),
                    "images": json.dumps(["https:" + image.get('imgUrl') for image in item.get('images', {})]),
                    "last_scrape_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "scrape_status": 'successful',
                }
            except KeyError as e:
                spider.logger.error(f"KeyError extracting fields for item: {e}")
                continue

            yield scrapy.Request(
                url=f"https://feedback.aliexpress.com/pc/searchEvaluation.do?productId={product['id']}",
                headers=response.headers,
                callback=parse_reviews,
                cb_kwargs={"product": product},
                meta={"impersonate": random.choice(self.browsers_list)},
            )
