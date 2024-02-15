import scrapy
from scrapy.spiders import CrawlSpider
from scrapy.linkextractors import LinkExtractor
import csv, re
from w3lib.url import url_query_cleaner


class NYTCrawler(CrawlSpider):
    name = 'nyt'
    allowed_domains = ['www.nytimes.com']
    start_urls = ['https://www.nytimes.com/']
    pp, mp = 0, 20000
    outsideUrls = set()

    def processLinks(self, links):
        for link in links:
            link.url = url_query_cleaner(link.url)
            if not re.match(r'\bhttps?://www\.nytimes\.com\S*', link.url):
                self.outsideUrls.add(link.url)
            else:
                yield link

    rules = (
        Rule(LinkExtractor(), callback='parseItem', follow=True),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetchCsvFile = open('fetch_nytimes.csv', 'w', newline='', encoding='utf-8')
        self.fetchCsvWriter = csv.writer(self.fetchCsvFile)
        self.fetchCsvWriter.writerow(['URL', 'Status'])

        self.visitCsvFile = open('visit_nytimes.csv', 'w', newline='', encoding='utf-8')
        self.visitCsvWriter = csv.writer(self.visitCsvFile)
        self.visitCsvWriter.writerow(['URL', 'Size (kB)', 'Outlinks', 'Content-Type'])

        self.urlsCsvFile = open('urls_nytimes.csv', 'w', newline='', encoding='utf-8')
        self.urlsCsvWriter = csv.writer(self.urlsCsvFile)
        self.urlsCsvWriter.writerow(['URL', 'Indicator'])

    def parseItem(self, response):
        if self.pp >= self.mp:
            self.crawler.engine.close_spider(self, 'Reached 20,000 pages limit')

        outlinks = LinkExtractor().extract_links(response)
        numOutlinks = len(outlinks)
        self.fetchCsvWriter.writerow([response.url, response.status])
        contentType = response.headers.get(b'Content-Type', b' ').decode('utf-8').split(';')[0]

        if response.status == 200:
            self.visitCsvWriter.writerow([
                response.url,
                len(response.body) / 1024,
                numOutlinks,
                contentType,
            ])

        if re.match(r'\bhttps?://www\.nytimes\.com\S*', response.url):
            self.urlsCsvWriter.writerow([response.url, 'OK'])
        else:
            self.urlsCsvWriter.writerow([response.url, 'N_OK'])

        self.pp += 1

    def closed(self, reason):
        self.fetchCsvFile.close()
        self.visitCsvFile.close()
        self.urlsCsvFile.close()
