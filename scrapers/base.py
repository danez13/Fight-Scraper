import os
import logging
from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    def __init__(
        self,
        base_url: str,
        wait_time: int,
        ignore_errors: bool,
        direct: bool,
        update: bool,
        events_file: str = "Events.csv",
        fights_file: str = "Fights.csv"
    ):
        self.base_url = base_url
        self.wait_time = wait_time
        self.ignore_errors = ignore_errors
        self.direct = direct
        self.update = update
        self.events_file = events_file
        self.fights_file = fights_file

        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept-Language": "en-US,en;q=0.9",
        }

        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        logger.debug("Initialized BeautifulSoup scraper with retry-enabled requests.Session")

    def fetch_soup(self, url: str) -> BeautifulSoup:
        try:
            logger.debug(f"Fetching URL: {url}")
            response = self.session.get(url, headers=self.headers, timeout=self.wait_time)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            raise e
    def parse_elements(self,soup: BeautifulSoup, selector: str) -> list:
        """Parse elements from the soup using a CSS selector."""
        elements = soup.select(selector)
        if elements:
            return elements
        logger.warning(f"No elements found for selector: {selector}")
        raise ValueError(f"No elements found for selector: {selector}")

    def parse_element(self, element: BeautifulSoup|Tag, selector: str) -> Tag:
        """Parse a single element using a CSS selector."""
        parsed_element = element.select_one(selector)
        if parsed_element:
            return parsed_element
        
        logger.warning(f"No element found for selector: {selector}")
        raise ValueError(f"No element found for selector: {selector}")

    def parse_Tag_attribute(self,element: Tag, attribute: str) -> str:
        """Parse an attribute from a BeautifulSoup Tag."""
        if element.has_attr(attribute):
            return str(element.get(attribute))

        logger.warning(f"Attribute '{attribute}' not found in element: {element}")
        raise ValueError(f"Attribute '{attribute}' not found in element: {element}")
    
    def parse_text(self, element: Tag) -> str:
        """Extract and clean text from a BeautifulSoup Tag."""
        if element and element.text:
            return element.text
        
        logger.warning(f"No text found in element: {element}")
        raise ValueError(f"No text found in element: {element}")

    def clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if text:
            return ' '.join(text.split()).strip()
        
        logger.warning("Empty or None text provided for cleaning")
        return ""
    
    def parse_id_from_url(self, url: str) -> str:
        """Extract an ID from a URL assuming the ID is the last segment."""
        if url:
            parts = url.rstrip('/').split('/')
            if parts:
                return parts[-1]
            else:
                raise ValueError(f"Could not extract ID from URL: {url}")
        
        logger.warning(f"Could not extract ID from URL: {url}")
        raise ValueError(f"Could not extract ID from URL: {url}")
    
    @abstractmethod
    def run(self):
        raise NotImplementedError("Subclasses must implement the run method.")

