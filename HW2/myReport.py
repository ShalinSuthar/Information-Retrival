import csv

def generate_crawl_report(news_site, fetch_path, visit_path, urls_path):

    fetch_counts = {"attempts": 0, "successes": 0, "failures": 0}
    outgoing_urls = {"total": 0, "inside": 0, "outside": 0}
    status_codes = {}
    file_sizes = {"< 1KB": 0, "1KB ~ <10KB": 0, "10KB ~ <100KB": 0, "100KB ~ <1MB": 0, ">= 1MB": 0}
    content_types = {}

    # Process fetch data
    with open(fetch_path, "r", newline="", encoding="utf-8") as fetch_file:
        reader = csv.reader(fetch_file)
        next(reader)  # Skip header
        for row in reader:
            fetch_counts["attempts"] += 1
            status_code = row[1]
            if status_code == "200":
                fetch_counts["successes"] += 1
            else:
                fetch_counts["failures"] += 1

            if status_code in status_codes:
                status_codes[status_code] += 1
            else:
                status_codes[status_code] = 1

    # Process visit data
    with open(visit_path, "r", newline="", encoding="utf-8") as visit_file:
        reader = csv.reader(visit_file)
        next(reader)  # Skip header
        for row in reader:
            outgoing_urls["total"] += 1
            if row[0].startswith(news_site):
                outgoing_urls["inside"] += 1
            else:
                outgoing_urls["outside"] += 1

            try:
                file_size = float(row[1]) * 1024
            except ValueError:
                continue

            if file_size < 1024:
                file_sizes["< 1KB"] += 1
            elif 1024 <= file_size < 10240:
                file_sizes["1KB ~ <10KB"] += 1
            elif 10240 <= file_size < 102400:
                file_sizes["10KB ~ <100KB"] += 1
            elif 102400 <= file_size < 1024 * 1024:
                file_sizes["100KB ~ <1MB"] += 1
            else:
                file_sizes[">= 1MB"] += 1

            content_type = row[3]
            if content_type in content_types:
                content_types[content_type] += 1
            else:
                content_types[content_type] = 1

    # Process URL data
    with open(urls_path, "r", newline="", encoding="utf-8") as urls_file:
        reader = csv.reader(urls_file)
        next(reader)  # Skip header
        for row in reader:
            if row[1] == "OK":
                outgoing_urls["inside"] += 1
            else:
                outgoing_urls["outside"] += 1

    # Generate report
    report_path = f"CrawlReport_{news_site}.txt"
    with open(report_path, "w") as report_file:
        report_file.write("Name: Shalin Suthar\n")
        report_file.write(f'USC ID: 8333139319\n')
        report_file.write(f'News site crawled: {news_site_name}\n')
        report_file.write(f'Number of threads: 1\n') 
        report_file.write(f'\nFetch Statistics\n')
        report_file.write(f'================\n')
        report_file.write(f'# fetches attempted: {fetch_attempts}\n')
        report_file.write(f'# fetches succeeded: {fetch_successes}\n')
        report_file.write(f'# fetches failed or aborted: {fetch_failures}\n')
        report_file.write(f'\nOutgoing URLs:\n')
        report_file.write(f'==============\n')
        report_file.write(f'Total URLs extracted: {unique_urls_total}\n')
        report_file.write(f'# unique URLs extracted: {outgoing_urls_inside}\n')
        report_file.write(f'# unique URLs within News Site: {outgoing_urls_inside}\n')
        report_file.write(f'# unique URLs outside News Site: {outgoing_urls_outside}\n')
        report_file.write(f'\nStatus Codes:\n')
        report_file.write(f'=============\n')
        for code, count in status_codes.items():
        report_file.write(f'{code}: {count}\n')
        report_file.write(f'\nFile Sizes:\n')
        report_file.write(f'===========\n')
        for size_range, count in file_sizes.items():
        report_file.write(f'{size_range}: {count}\n')
        report_file.write(f'\nContent Types:\n')
        report_file.write(f'===============\n')
        for content_type, count in content_types.items():
        report_file.write(f'{content_type}: {count}\n')
