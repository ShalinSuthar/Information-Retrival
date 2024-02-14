from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import csv, re
from w3lib.url import url_query_cleaner

class NYTCrawler(CrawlSpider):
    name = 'nyt'
    start_urls = ['https://www.nytimes.com/']
    max_pages = 20000
    processed_pages = 0
    outside_urls = set()

    def process_links(self, links):
        for link in links:
            link.url = url_query_cleaner(link.url)
            if not re.match(r'\bhttps?://www\.nytimes\.com\S*', link.url):
                self.outside_urls.add(link.url)
            else:
                yield link

    rules = (
        Rule(LinkExtractor(), process_links='process_links', callback='parse_item', follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(NYTCrawler, self).__init__(*args, **kwargs)
        self.fetch_csv_file = open('fetch_nytimes.csv', 'w', newline='', encoding='utf-8')
        self.fetch_csv_writer = csv.writer(self.fetch_csv_file)
        self.fetch_csv_writer.writerow(['URL', 'Status'])

        self.visit_csv_file = open('visit_nytimes.csv', 'w', newline='', encoding='utf-8')
        self.visit_csv_writer = csv.writer(self.visit_csv_file)
        self.visit_csv_writer.writerow(['URL', 'Size (kB)', 'Outlinks', 'Content-Type'])

        self.urls_csv_file = open('urls_nytimes.csv', 'w', newline='', encoding='utf-8')
        self.urls_csv_writer = csv.writer(self.urls_csv_file)
        self.urls_csv_writer.writerow(['URL', 'Indicator'])

    def parse_item(self, response):
        if self.processed_pages >= self.max_pages:
            self.crawler.engine.close_spider(self, 'Reached 20,000 pages limit')

        outlinks = LinkExtractor().extract_links(response)
        no_outlinks = len(outlinks)
        self.fetch_csv_writer.writerow([response.url, response.status])
        content_type = response.headers.get(b'Content-Type', b' ').decode('utf-8').split(';')[0]
        if response.status == 200:
            self.visit_csv_writer.writerow([
                response.url,
                len(response.body) / 1024,  
                no_outlinks,
                content_type
            ])

        
        if re.match(r'\bhttps?://www\.nytimes\.com\S*', response.url):
            self.urls_csv_writer.writerow([response.url, 'OK'])
        else:
            self.urls_csv_writer.writerow([response.url, 'N_OK'])

        self.processed_pages += 1

    def closed(self, reason):
        self.fetch_csv_file.close()
        self.visit_csv_file.close()
        self.urls_csv_file.close()
