# aliexpress_scraper
A scrapy framework for scraping AliExpress. 
To scrape AliExpress, make your search string by typing the keywords in the AliExpress search bar and selecting the other query options and pressing the search button. Then copy the string in the browser address bar and pasting as follows:
scrapy crawl aliexpress -a "YOUR_COPIED_TEXT_SITS_HERE"

The script gives the scraped data in two formats: 
1. Sqlite3  
2. jason
Other formats also can easily be made.
