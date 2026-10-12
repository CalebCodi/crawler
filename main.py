import sys
from crawl import crawl_site_async
import asyncio
from json_report import write_json_report

async def main():
    if len(sys.argv) < 2:
        print("no website provided")
        sys.exit(1)
    elif len(sys.argv) > 4:
        print("too many arguments provided")
        sys.exit(1)

    base_url = sys.argv[1]

    # Parse optional CLI arguments with sensible defaults
    max_concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    max_pages = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    print(f"starting crawl of: {base_url}")

    page_data = await crawl_site_async(
        base_url,
        max_concurrency=max_concurrency,
        max_pages=max_pages
    )

    write_json_report(page_data)


if __name__ == "__main__":
    asyncio.run(main())
