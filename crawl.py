import asyncio
import aiohttp
from urllib.parse import urlsplit, urljoin
from bs4 import BeautifulSoup, Tag
import requests

def normalize_url(url: str) -> str:
    o = urlsplit(url)
    return o.netloc + o.path.rstrip("/")


def get_heading_from_html(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')

    h1 = soup.find('h1')
    if isinstance(h1, Tag):
        return h1.get_text(strip=True)

    h2 = soup.find('h2')
    if isinstance(h2, Tag):
        return h2.get_text(strip=True)

    return ""


def get_first_paragraph_from_html(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')

    p = soup.select_one('main p')
    if isinstance(p, Tag):
        return p.get_text(strip=True)

    p = soup.find('p')
    if isinstance(p, Tag):
        return p.get_text(strip=True)

    return ""

def get_urls_from_html(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    urls = []

    # Check all <a> tags
    for a_tag in soup.find_all('a'):
        if isinstance(a_tag, Tag):
            href = a_tag.get('href')
            if href:
                # urljoin handles both absolute and relative links automatically
                absolute_url = urljoin(base_url, href)
                urls.append(absolute_url)

    return urls

def get_images_from_html(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    image_urls = []

    # Check all <img> tags
    for img_tag in soup.find_all('img'):
        if isinstance(img_tag, Tag):
            src = img_tag.get('src')
            if src:
                # urljoin handles both absolute and relative image paths automatically
                absolute_url = urljoin(base_url, src)
                image_urls.append(absolute_url)

    return image_urls

def extract_page_data(html: str, page_url: str):
    return {
        "url": page_url,
        "heading": get_heading_from_html(html),
        "first_paragraph": get_first_paragraph_from_html(html),
        "outgoing_links": get_urls_from_html(html, page_url),
        "image_urls": get_images_from_html(html, page_url),
    }



class AsyncCrawler():
    def __init__(self, base_url: str, max_concurrency = 1, max_pages = 10):
        self.base_url = base_url
        self.base_domain = urlsplit(base_url).netloc
        self.page_data = {}
        self.visited = set()
        self.lock = asyncio.Lock()
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self.session = None
        self.max_pages = max_pages
        self.should_stop = False
        self.all_tasks = set()

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def add_page_visit(self, normalized_url):
        async with self.lock:
            if self.should_stop:
                return False

            if normalized_url in self.visited:
                return False

            if len(self.visited) >= self.max_pages:
                self.should_stop = True
                print("Reached maximum number of pages to crawl.")
                return False

            self.visited.add(normalized_url)
            return True

    async def get_html(self, url: str) -> str:
        headers = {"User-Agent": "BootCrawler/1.0"}
        async with self.session.get(url, headers=headers) as response:
            if response.status >= 400:
                raise Exception(f"HTTP error: status code {response.status}")

            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type:
                raise Exception(f"Invalid Content-Type: expected text/html, got {content_type}")

            return await response.text()

    async def crawl_page(self, current_url):
        if self.should_stop:
            return

        if self.base_domain != urlsplit(current_url).netloc:
            return

        norm = normalize_url(current_url)
        if not await self.add_page_visit(norm):
            return

        try:
            async with self.semaphore:
                print(f"Getting data from: {current_url}")
                html = await self.get_html(current_url)
                page_info = extract_page_data(html, current_url)
        except Exception as e:
            print(f"Error crawling {current_url}: {e}")
            return

        async with self.lock:
            self.page_data[norm] = page_info

        tasks = []
        for link in page_info["outgoing_links"]:
            if self.should_stop:
                break

            task = asyncio.create_task(self.crawl_page(link))
            self.all_tasks.add(task)
            # Remove task from set when completed
            task.add_done_callback(self.all_tasks.discard)
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def crawl(self) -> dict:
        await self.crawl_page(self.base_url)
        return self.page_data


async def crawl_site_async(base_url: str, max_concurrency: int = 1, max_pages: int = 10) -> dict:
    async with AsyncCrawler(base_url, max_concurrency=max_concurrency, max_pages=max_pages) as crawler:
        return await crawler.crawl()
