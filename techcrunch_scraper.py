import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
from datetime import datetime
import argparse

def fetch_html(url, timeout=10):
    """Fetch raw HTML for a given URL."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return None

def parse_homepage(url, html, allowed_prefix=None):
    soup  = BeautifulSoup(html, 'html.parser')
    links = set()

    for a in soup.find_all('a', href=True):
        if a.find_parent('footer'):
            continue

        full_url = urljoin(url, a['href'])
        if allowed_prefix and not full_url.startswith(allowed_prefix):
            continue

        links.add(full_url)

    return list(links)


def extract_article(url, html):
    """Extract metadata and cleaned text from an article page."""
    soup = BeautifulSoup(html, 'html.parser')

    # Metadata extraction
    title_tag = soup.find('h1', class_='article__title') or soup.find('h1')
    title = title_tag.get_text(strip=True) if title_tag else ''

    author_tag = soup.find('a', rel='author') or soup.select_one('span.river-byline__authors')
    author = author_tag.get_text(strip=True) if author_tag else ''

    date_tag = soup.find('time')
    published = date_tag.get('datetime') if date_tag and date_tag.get('datetime') else ''

    # Robust body extraction with multiple selectors
    selectors = [
        'div.article-content',
        'div.article__content',
        'div.article-content__container',
        'div[data-test-id="post-content"]',
        'article',
        'main'
    ]
    article_body = None
    for sel in selectors:
        if sel.startswith('div['):
            article_body = soup.select_one(sel)
        else:
            parts = sel.split('.', 1)
            if len(parts) == 2:
                tag, cls = parts
                article_body = soup.find(tag, class_=cls)
            else:
                article_body = soup.find(sel)
        if article_body:
            break

    paragraphs = []
    if article_body:
        for p in article_body.find_all('p'):
            text = p.get_text(strip=True)
            if text:
                paragraphs.append(text)

    clean_text = "\n\n".join(paragraphs)

    return {
        'title':      title,
        'author':     author,
        'published':  published,
        'clean_text': clean_text
    }

def run_scraper(homepage_url, output_path):
    scraped_urls = set()
    page = 1
    record_counter = 1

    domain = urlparse(homepage_url).netloc
    allowed_prefix = f"https://{domain}/20"

    with open(output_path, 'w', encoding='utf-8') as out_f:
        while True:
            if page == 1:
                page_url = homepage_url
            else:
                page_url = urljoin(homepage_url.rstrip('/') + '/', f'page/{page}/')

            if page == 3:
                break

            print(f"Fetching page {page}: {page_url}")
            homepage_html = fetch_html(page_url)
            if not homepage_html:
                print(f"No HTML fetched for {page_url}, stopping.")
                break

            article_urls = parse_homepage(page_url, homepage_html, allowed_prefix=allowed_prefix)
            new_urls = [u for u in article_urls if u not in scraped_urls]

            if not new_urls:
                print(f"No new date‐style articles found on page {page}, stopping.")
                break

            print(f"  → Found {len(new_urls)} new articles on page {page}.")

            for article_url in new_urls:
                html = fetch_html(article_url)
                if not html:
                    continue

                meta = extract_article(article_url, html)
                record = {
                    'id':         f"tc-{datetime.now().strftime('%Y%m%d')}-{record_counter:03d}",
                    'source':     domain,
                    'url':        article_url,
                    'scraped_at': datetime.now().astimezone().isoformat(),
                    'title':      meta['title'],
                    'author':     meta['author'],
                    'published':  meta['published'],
                    'raw_html':   html,
                    'clean_text': meta['clean_text'].replace("\n", "")
                }
                out_f.write(json.dumps(record, ensure_ascii=False) + '\n')
                scraped_urls.add(article_url)
                record_counter += 1

            page += 1

    print(f"Scraping complete. Total articles: {len(scraped_urls)}. Output written to {output_path}.")

    print(meta['clean_text'].replace("\n", ""))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TechCrunch startups scraper')
    parser.add_argument(
        '--url',
        default='https://techcrunch.com/category/startups/',
        help='Category URL (e.g., TechCrunch startups)'
    )
    parser.add_argument(
        '-o', '--output',
        default='prototype_output.jsonl',
        help='Path to output JSONL file'
    )
    args = parser.parse_args()
    run_scraper(args.url, args.output)