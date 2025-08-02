import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import re
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
    """Return all article‐style links matching allowed_prefix, skipping footer links."""
    soup = BeautifulSoup(html, 'html.parser')
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

    title_tag = soup.find('h1', class_='article__title') or soup.find('h1')
    title = title_tag.get_text(strip=True) if title_tag else ''

    # New, multi-step author extraction:
    author = ''
    # 1) meta tag
    m = soup.find('meta', attrs={'name': 'author'})
    if m and m.get('content'):
        author = m['content'].strip()
    else:
        # 2) any <a rel="author">
        a = soup.find('a', rel='author')
        if a and a.get_text(strip=True):
            author = a.get_text(strip=True)
        else:
            # 3) inside .river-byline__authors container
            link = soup.select_one('.river-byline__authors a')
            if link and link.get_text(strip=True):
                author = link.get_text(strip=True)
            else:
                # 4) fallback: look for "By X Y"
                byline = soup.find(text=re.compile(r'^\s*By\s+'))
                if byline:
                    author = byline.strip().lstrip('By ').strip()

    date_tag = soup.find('time')
    published = date_tag.get('datetime') if date_tag and date_tag.get('datetime') else ''

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
            text = p.get_text(separator=' ', strip=True)
            if text:
                paragraphs.append(text)

    clean_text = " ".join(paragraphs)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    clean_text = re.sub(r'\s+([.,!?:;])', r'\1', clean_text)
    clean_text = re.sub(r'\(\s+', '(', clean_text)
    clean_text = re.sub(r'\s+\)', ')', clean_text)

    return {
        'title':      title,
        'author':     author,
        'published':  published,
        'clean_text': clean_text
    }

def run_scraper(categories, output_path):
    domain = "techcrunch.com"
    allowed_prefix = f"https://{domain}/20"
    scraped_urls = set()
    record_counter = 1

    with open(output_path, 'w', encoding='utf-8') as out_f:
        for category in categories:
            homepage_url = f"https://{domain}/category/{category}/"
            for page in range(1, 334):  # pages 1–333
                if page == 1:
                    page_url = homepage_url
                else:
                    page_url = urljoin(homepage_url.rstrip('/') + '/', f'page/{page}/')

                print(f"Fetching page {page}: {page_url}")
                html = fetch_html(page_url)
                if not html:
                    print(f"No HTML fetched for {page_url}, stopping {category}.")
                    break

                article_urls = parse_homepage(page_url, html, allowed_prefix=allowed_prefix)
                new_urls = [u for u in article_urls if u not in scraped_urls]

                if not new_urls:
                    print(f"No new date‐style articles found on page {page}, stopping {category}.")
                    break

                print(f"  → Found {len(new_urls)} new articles on page {page} of the {category} category.")

                for article_url in new_urls:
                    art_html = fetch_html(article_url)
                    if not art_html:
                        continue
                    meta = extract_article(article_url, art_html)
                    record = {
                        'id':         f"tc-{datetime.now().strftime('%Y%m%d')}-{record_counter:03d}",
                        'source':     domain,
                        'url':        article_url,
                        'title':      meta['title'],
                        'author':     meta['author'],
                        'published':  meta['published'],
                        'scraped_at': datetime.now().astimezone().isoformat(),
                        'clean_text': meta['clean_text']
                    }
                    out_f.write(json.dumps(record, ensure_ascii=False) + '\n')
                    scraped_urls.add(article_url)
                    record_counter += 1

    print(f"Scraping complete. Total unique articles: {len(scraped_urls)}. Output written to {output_path}.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TechCrunch startups & venture scraper')
    parser.add_argument(
        '-o', '--output',
        default='prototype_output.jsonl',
        help='Path to output JSONL file'
    )
    args = parser.parse_args()

    categories = ['startups', 'venture']
    run_scraper(categories, args.output)