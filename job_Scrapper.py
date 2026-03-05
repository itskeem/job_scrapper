"""
Job Scraper 
Comprehensive job scraper with improved HTTP fetching, error handling, and database storage.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import csv
import logging
import argparse
import time
import pandas as pd
from http_fetcher import HTTPFetcher
from typing import List, Dict, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JobScraper:
    """
    Comprehensive job scraper with HTTP fetching, HTML parsing, and data storage.
    
    Features:
    - Robust HTTP fetching with retry logic
    - HTML parsing with BeautifulSoup
    - SQLite database storage
    - CSV export functionality
    - Comprehensive logging
    - Pagination support
    """
    
    def __init__(self, db_name: str = 'jobs.db', timeout: int = 10, max_retries: int = 3):
        """
        Initialize the job scraper.
        
        Args:
            db_name: SQLite database filename
            timeout: HTTP request timeout in seconds
            max_retries: Maximum number of HTTP retries
        """
        self.db_name = db_name
        self.fetcher = HTTPFetcher(timeout=timeout, max_retries=max_retries)
        self._init_db()
        logger.info(f"JobScraper initialized with database: {db_name}")

    def _init_db(self):
        """Initialize SQLite database with jobs table."""
        try:
            with sqlite3.connect(self.db_name) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        company TEXT NOT NULL,
                        location TEXT,
                        salary TEXT,
                        date_posted TEXT,
                        url TEXT UNIQUE NOT NULL,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
                logger.info(f"Database initialized: {self.db_name}")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch a page using the HTTP fetcher.
        
        Args:
            url: URL to fetch
        
        Returns:
            HTML content if successful, None otherwise
        """
        return self.fetcher.fetch(url)

    def parse_jobs(self, html: str) -> List[Dict]:
        """
        Parse job listings from HTML content.
        
        Args:
            html: HTML content to parse
        
        Returns:
            List of job dictionaries
        """
        if not html:
            return []
        
        jobs = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Target elements based on typical job board structures
            # These selectors are common examples and may need adjustment for specific sites
            for item in soup.select('.job-card, .job-listing, .result, [data-job-id]'):
                try:
                    # Extract job information with fallbacks
                    title_elem = item.select_one('h2, .title, [data-job-title]')
                    company_elem = item.select_one('.company, .employer, [data-company]')
                    location_elem = item.select_one('.location, [data-location]')
                    salary_elem = item.select_one('.salary, [data-salary]')
                    date_elem = item.select_one('.date, .posted-date, [data-posted-date]')
                    url_elem = item.select_one('a')
                    
                    # Skip if essential fields are missing
                    if not title_elem or not company_elem:
                        logger.debug("Skipping job entry: missing title or company")
                        continue
                    
                    job = {
                        'title': title_elem.get_text(strip=True),
                        'company': company_elem.get_text(strip=True),
                        'location': location_elem.get_text(strip=True) if location_elem else 'N/A',
                        'salary': salary_elem.get_text(strip=True) if salary_elem else 'Not specified',
                        'date_posted': date_elem.get_text(strip=True) if date_elem else 'Recent',
                        'url': url_elem['href'] if url_elem and 'href' in url_elem.attrs else ''
                    }
                    
                    # Validate URL
                    if not job['url']:
                        logger.debug("Skipping job entry: missing URL")
                        continue
                    
                    jobs.append(job)
                    logger.debug(f"Parsed job: {job['title']} at {job['company']}")
                    
                except Exception as e:
                    logger.debug(f"Error parsing job entry: {e}")
                    continue
            
            logger.info(f"Parsed {len(jobs)} jobs from HTML")
            return jobs
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return []

    def save_to_db(self, jobs: List[Dict]) -> int:
        """
        Save jobs to SQLite database.
        
        Args:
            jobs: List of job dictionaries
        
        Returns:
            Number of jobs successfully saved
        """
        if not jobs:
            logger.warning("No jobs to save")
            return 0
        
        saved_count = 0
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                for job in jobs:
                    try:
                        cursor.execute('''
                            INSERT OR IGNORE INTO jobs 
                            (title, company, location, salary, date_posted, url)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            job['title'],
                            job['company'],
                            job['location'],
                            job['salary'],
                            job['date_posted'],
                            job['url']
                        ))
                        if cursor.rowcount > 0:
                            saved_count += 1
                    except sqlite3.IntegrityError:
                        logger.debug(f"Duplicate URL found: {job['url']}")
                    except Exception as e:
                        logger.error(f"Error saving job to DB: {e}")
                
                conn.commit()
                logger.info(f"Saved {saved_count} new jobs to database")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
        
        return saved_count

    def export_to_csv(self, filename: str = 'jobs.csv') -> bool:
        """
        Export jobs from database to CSV file.
        
        Args:
            filename: Output CSV filename
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_name) as conn:
                df = pd.read_sql_query("SELECT * FROM jobs ORDER BY scraped_at DESC", conn)
                
                if df.empty:
                    logger.warning("No jobs to export")
                    return False
                
                df.to_csv(filename, index=False)
                logger.info(f"Exported {len(df)} jobs to {filename}")
                return True
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            return False

    def get_job_count(self) -> int:
        """Get total number of jobs in database."""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM jobs")
                count = cursor.fetchone()[0]
                return count
        except Exception as e:
            logger.error(f"Error getting job count: {e}")
            return 0

    def run(self, base_url: str, pages: int = 1, delay: float = 1.0) -> int:
        """
        Run the scraper for multiple pages.
        
        Args:
            base_url: Base URL to scrape
            pages: Number of pages to scrape
            delay: Delay between requests in seconds
        
        Returns:
            Total number of new jobs scraped
        """
        total_jobs = 0
        
        for p in range(1, pages + 1):
            # Construct URL with page parameter
            url = f"{base_url}?page={p}" if "?" not in base_url else f"{base_url}&page={p}"
            
            logger.info(f"Scraping page {p}/{pages}: {url}")
            
            # Fetch page with delay
            html = self.fetcher.fetch_with_delay(url, delay=delay)
            
            if html:
                # Parse jobs from HTML
                jobs = self.parse_jobs(html)
                
                # Save to database
                saved = self.save_to_db(jobs)
                total_jobs += saved
                
                # Rotate user agent occasionally
                if p % 5 == 0:
                    self.fetcher.rotate_user_agent()
            else:
                logger.warning(f"Failed to fetch page {p}")
        
        logger.info(f"Scraping complete. Total new jobs: {total_jobs}")
        return total_jobs

    def close(self):
        """Close resources."""
        self.fetcher.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def main():
    """Command-line interface for the job scraper."""
    parser = argparse.ArgumentParser(
        description='Job Scraper - Comprehensive web scraping tool for job listings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Scrape a single page
  python job_scraper.py --url "https://example.com/jobs"
  
  # Scrape multiple pages
  python job_scraper.py --url "https://example.com/jobs" --pages 5
  
  # Export to CSV
  python job_scraper.py --url "https://example.com/jobs" --pages 3 --export jobs.csv
  
  # Custom database and timeout
  python job_scraper.py --url "https://example.com/jobs" --db my_jobs.db --timeout 15
        '''
    )
    
    parser.add_argument('--url', type=str, help='Base URL to scrape')
    parser.add_argument('--pages', type=int, default=1, help='Number of pages to scrape (default: 1)')
    parser.add_argument('--export', type=str, help='CSV filename to export results')
    parser.add_argument('--db', type=str, default='jobs.db', help='SQLite database filename (default: jobs.db)')
    parser.add_argument('--timeout', type=int, default=10, help='HTTP request timeout in seconds (default: 10)')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between requests in seconds (default: 1.0)')
    parser.add_argument('--retries', type=int, default=3, help='Maximum HTTP retries (default: 3)')
    parser.add_argument('--count', action='store_true', help='Show job count and exit')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = JobScraper(
        db_name=args.db,
        timeout=args.timeout,
        max_retries=args.retries
    )
    
    try:
        # Show job count if requested
        if args.count:
            count = scraper.get_job_count()
            print(f"Total jobs in database: {count}")
            return
        
        # Run scraper if URL provided
        if args.url:
            scraper.run(args.url, pages=args.pages, delay=args.delay)
        
        # Export to CSV if requested
        if args.export:
            scraper.export_to_csv(args.export)
        
        # Show final count
        count = scraper.get_job_count()
        print(f"\nTotal jobs in database: {count}")
        
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
