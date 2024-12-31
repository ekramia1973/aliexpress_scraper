# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class AliexpressSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class AliexpressDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called

        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

from curl_cffi import requests, Curl, CurlOpt
from scrapy.http import HtmlResponse
from scrapy.exceptions import NotConfigured
from loguru import logger
class CurlMiddleware:
    def __init__(self):
        self.session = requests.Session()

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('CURL_ENABLED'):
            raise NotConfigured
        return cls()

    def process_request(self, request, spider):
        c=Curl()
        c.setopt(CurlOpt.HTTP2_PSEUDO_HEADERS_ORDER, "masp")
        try:
            curl_response = self.session.request(
                method=request.method,
                url=request.url,
                headers=request.headers.to_unicode_dict(),
                cookies=request.cookies,
                data=request.body,
                timeout=(10, 30),  
                allow_redirects=True,
                max_redirects=3,
                # curl_options={
                #             CurlOpt.HTTP_VERSION: Curl.HTTP_VERSION_2,
                #             CurlOpt.HTTP2_PSEUDO_HEADERS_ORDER: "masp",
                # }
            )
            # print("*****HEADERS: ", curl_response.request.headers)
            return HtmlResponse(
                url=request.url,
                body=curl_response.content,
                encoding='utf-8',
                request=request,
                status=curl_response.status_code
            )
        except Exception as e:
            spider.logger.error(f'CurlMiddleware error: {e}')
            return None

from urllib.parse import urlencode
from random import choice
import requests as _requests
from scrapy.exceptions import NotConfigured
from scrapy import signals

# from scrapy.signalmanager import dispatcher
class RandomUserAgentMiddleware:

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('RANDOM_USER_AGENTS_ENABLED', True):
            raise NotConfigured
        middleware = cls(crawler.settings)
        # dispatcher.connect(self.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def __init__(self, settings):
        self.random_headers_api_key = settings.get('SCRAPEOPS_API_KEY')
        self.number_of_random_user_agents = settings.getint('NUMBER_OF_RANDOM_USER_AGENTS', 10)
        self.endpoint = settings.get('SCRAPEOPS_API_ENDPOINT', 'https://headers.scrapeops.io/v1/user-agents')
        self.user_agents = []
        self.retries = 3

    def spider_opened(self, spider):
        self._get_random_user_agents_list()

    def _get_random_user_agents_list(self):
        payload = {
            'api_key': self.random_headers_api_key,
            'num_results': self.number_of_random_user_agents
        }
        for _ in range(self.retries):
            try:
                response = _requests.get(self.endpoint, params=urlencode(payload), timeout=10)
                response.raise_for_status()
                json_response = response.json()
                self.user_agents = json_response.get('result', [])
                if self.user_agents:
                    break
            except (_requests.RequestException, ValueError) as e:
                self.logger.error(f"Error fetching user agents: {e}")
        if not self.user_agents:
            self.logger.error("Failed to fetch user agents after multiple attempts")

    def process_request(self, request, spider):
        if self.user_agents:
            request.headers['User-Agent'] = choice(self.user_agents)
        else:
            spider.logger.warning("No user agents available, using default")        
        return None
