"""
HTTP Fetcher Module
Enhanced HTTP fetching with retry logic, session management, and robust error handling.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import time
from typing import Optional, Dict
from random import choice

logger = logging.getLogger(__name__)

class HTTPFetcher:
    """
    Robust HTTP fetcher with retry logic, session management, and error handling.
    
    Features:
    - Automatic retry on network failures
    - Custom user-agent rotation
    - Session pooling for connection reuse
    - Configurable timeouts
    - Response validation
    """
    
    # List of common user agents to rotate through
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59',
    ]
    
    def __init__(self, timeout: int = 10, max_retries: int = 3, backoff_factor: float = 0.5):
        """
        Initialize the HTTP fetcher with session and retry configuration.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            backoff_factor: Backoff factor for exponential backoff
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry strategy and connection pooling.
        
        Returns:
            Configured requests.Session object
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        
        # Mount adapter to both HTTP and HTTPS
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'User-Agent': choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        return session
    
    def fetch(self, url: str, headers: Optional[Dict] = None, verify_ssl: bool = True) -> Optional[str]:
        """
        Fetch a URL with error handling and retry logic.
        
        Args:
            url: URL to fetch
            headers: Optional custom headers to merge with defaults
            verify_ssl: Whether to verify SSL certificates
        
        Returns:
            Response text if successful, None otherwise
        """
        try:
            # Merge custom headers if provided
            request_headers = self.session.headers.copy()
            if headers:
                request_headers.update(headers)
            
            logger.debug(f"Fetching URL: {url}")
            
            response = self.session.get(
                url,
                headers=request_headers,
                timeout=self.timeout,
                verify=verify_ssl,
                allow_redirects=True
            )
            
            # Raise exception for bad status codes
            response.raise_for_status()
            
            # Validate response
            if not response.text:
                logger.warning(f"Empty response from {url}")
                return None
            
            logger.info(f"Successfully fetched {url} (Status: {response.status_code})")
            return response.text
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching {url} (timeout: {self.timeout}s)")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error while fetching {url}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} while fetching {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching {url}: {e}")
            return None
    
    def fetch_with_delay(self, url: str, delay: float = 1.0, headers: Optional[Dict] = None) -> Optional[str]:
        """
        Fetch a URL with a delay before the request (for polite scraping).
        
        Args:
            url: URL to fetch
            delay: Delay in seconds before making the request
            headers: Optional custom headers
        
        Returns:
            Response text if successful, None otherwise
        """
        logger.debug(f"Waiting {delay}s before fetching {url}")
        time.sleep(delay)
        return self.fetch(url, headers=headers)
    
    def rotate_user_agent(self):
        """Rotate the user agent in the session."""
        self.session.headers['User-Agent'] = choice(self.USER_AGENTS)
        logger.debug(f"Rotated user agent to: {self.session.headers['User-Agent'][:50]}...")
    
    def close(self):
        """Close the session and cleanup resources."""
        self.session.close()
        logger.info("HTTP session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example 1: Simple fetch
    fetcher = HTTPFetcher(timeout=10, max_retries=3)
    html = fetcher.fetch("https://example.com")
    if html:
        print(f"Fetched {len(html)} bytes")
    fetcher.close()
    
    # Example 2: Using context manager
    with HTTPFetcher() as fetcher:
        html = fetcher.fetch_with_delay("https://example.com", delay=1.0)
        if html:
            print(f"Fetched {len(html)} bytes")


