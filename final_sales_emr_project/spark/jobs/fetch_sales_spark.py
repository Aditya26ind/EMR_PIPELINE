"""
PySpark job to crawl books.toscrape.com and write Parquet to S3.

This job is intentionally DRIVER-ONLY for crawling
(because the site is small and this is a sandbox scraper).

EMR / MWAA safe.
"""

import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from urllib.parse import urljoin
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit


# -------------------------
# Argument parsing
# -------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Scrape books.toscrape.com and write to S3")
    parser.add_argument(
        "--start_url",
        required=True,
        help="Start URL (e.g. https://books.toscrape.com/)"
    )
    parser.add_argument(
        "--output_s3",
        required=True,
        help="S3 output path (e.g. s3://bucket/output/books/)"
    )
    parser.add_argument(
        "--max_pages",
        type=int,
        default=0,
        help="Maximum pages to crawl (0 = no limit)"
    )
    return parser.parse_args()


# -------------------------
# HTML extraction helpers
# -------------------------
def extract_book_info(article, base_url):
    title = article.select_one("h3 a")["title"] if article.select_one("h3 a") else None
    price = article.select_one(".price_color").get_text(strip=True) if article.select_one(".price_color") else None
    availability = article.select_one(".availability").get_text(strip=True) if article.select_one(".availability") else None

    rating = None
    rating_el = article.select_one("p.star-rating")
    if rating_el:
        for cls in rating_el.get("class", []):
            if cls != "star-rating":
                rating = cls

    rel = article.select_one("h3 a")["href"] if article.select_one("h3 a") else None
    product_page = urljoin(base_url, rel) if rel else None

    return {
        "title": title,
        "price": price,
        "availability": availability,
        "rating": rating,
        "product_page": product_page,
        "scraped_at": datetime.utcnow().isoformat()
    }


# -------------------------
# Crawl logic (driver-only)
# -------------------------
def crawl(start_url, max_pages=0):
    results = []
    url = start_url
    page_count = 0

    while url:
        page_count += 1
        if max_pages and page_count > max_pages:
            break

        print(f"[INFO] Crawling page {page_count}: {url}")

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.select(".product_pod")

        for article in articles:
            results.append(extract_book_info(article, url))

        next_link = soup.select_one("li.next a")
        url = urljoin(url, next_link["href"]) if next_link else None

    print(f"[INFO] Total records scraped: {len(results)}")
    return results


# -------------------------
# Main Spark entrypoint
# -------------------------
def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("books_to_scrape_pipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.speculation", "false")
        .getOrCreate()
    )

    # Crawl on driver
    records = crawl(args.start_url, args.max_pages)

    if not records:
        print("[WARN] No records scraped. Exiting job.")
        spark.stop()
        return

    df = spark.createDataFrame(records)

    # Controlled partition date (job run date)
    run_date = date.today().isoformat()
    df = df.withColumn("date", to_date(lit(run_date)))

    (
        df.write
        .mode("append")
        .partitionBy("date")
        .parquet(args.output_s3)
    )

    print("[INFO] Write completed successfully.")
    spark.stop()


# -------------------------
# Entry guard
# -------------------------
if __name__ == "__main__":
    main()
