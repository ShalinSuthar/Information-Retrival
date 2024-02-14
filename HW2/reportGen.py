import csv

def generate_crawl_report(news_site_name, fetch_csv_path, visit_csv_path, urls_csv_path):
    
    fetch_attempts = 0
    fetch_successes = 0
    fetch_failures = 0
    outgoing_urls_total = 0
    outgoing_urls_inside = 0
    outgoing_urls_outside = 0
    status_codes = {}
    file_sizes = {
        '< 1KB': 0,
        '1KB ~ <10KB': 0,
        '10KB ~ <100KB': 0,
        '100KB ~ <1MB': 0,
        '>= 1MB': 0
    }
    content_types = {}

    
    with open(fetch_csv_path, 'r', newline='', encoding='utf-8') as fetch_csv:
        fetch_reader = csv.reader(fetch_csv)
        next(fetch_reader) 
        for row in fetch_reader:
            fetch_attempts += 1
            status_code = row[1]
            if status_code == '200':
                fetch_successes += 1
            else:
                fetch_failures += 1

            
            if status_code in status_codes:
                status_codes[status_code] += 1
            else:
                status_codes[status_code] = 1

    
    with open(visit_csv_path, 'r', newline='', encoding='utf-8') as visit_csv:
        visit_reader = csv.reader(visit_csv)
        next(visit_reader)  
        for row in visit_reader:
            outgoing_urls_total += 1
            if row[0].startswith(news_site_name):
                outgoing_urls_inside += 1
            else:
                outgoing_urls_outside += 1

            
            try:
                file_size = float(row[1]) 
            except ValueError:
                continue  

            
            if file_size < 1024:
                file_sizes['< 1KB'] += 1
            elif 1024 <= file_size < 10240:
                file_sizes['1KB ~ <10KB'] += 1
            elif 10240 <= file_size < 102400:
                file_sizes['10KB ~ <100KB'] += 1
            elif 102400 <= file_size < 1024 * 1024:
                file_sizes['100KB ~ <1MB'] += 1
            else:
                file_sizes['>= 1MB'] += 1

            
            content_type = row[3]  
            if content_type in content_types:
                content_types[content_type] += 1
            else:
                content_types[content_type] = 1

    
    inside_urls = 0
    outside_urls = 0
    with open(urls_csv_path, 'r', newline='', encoding='utf-8') as urls_csv:
        urls_reader = csv.reader(urls_csv)
        next(urls_reader) 
        for row in urls_reader:
            if row[1] == 'OK':
                inside_urls += 1
            else:
                outside_urls += 1

    
    report_file_path = f'CrawlReport_{news_site_name}.txt'
    with open(report_file_path, 'w') as report_file:
        report_file.write(f'Name: Tommy Trojan\n')
        report_file.write(f'USC ID: 1234567890\n')
        report_file.write(f'News site crawled: {news_site_name}\n')
        report_file.write(f'Number of threads: 7\n') 
        report_file.write(f'Fetch Statistics\n')
        report_file.write(f'================\n')
        report_file.write(f'# fetches attempted: {fetch_attempts}\n')
        report_file.write(f'# fetches succeeded: {fetch_successes}\n')
        report_file.write(f'# fetches failed or aborted: {fetch_failures}\n')
        report_file.write(f'Outgoing URLs:\n')
        report_file.write(f'==============\n')
        report_file.write(f'Total URLs extracted: {outgoing_urls_total}\n')
        report_file.write(f'# unique URLs within News Site: {outgoing_urls_inside}\n')
        report_file.write(f'# unique URLs outside News Site: {outgoing_urls_outside}\n')
        report_file.write(f'Status Codes:\n')
        report_file.write(f'=============\n')
        for code, count in status_codes.items():
            report_file.write(f'{code}: {count}\n')
        report_file.write(f'File Sizes:\n')
        report_file.write(f'===========\n')
        for size_range, count in file_sizes.items():
            report_file.write(f'{size_range}: {count}\n')
        report_file.write(f'Content Types:\n')
        report_file.write(f'===============\n')
        for content_type, count in content_types.items():
            report_file.write(f'{content_type}: {count}\n')

    print(f'Report generated: {report_file_path}')


generate_crawl_report('nytimes.com', 'fetch_nytimes.csv', 'visit_nytimes.csv', 'urls_nytimes.csv')
