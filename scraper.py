"""
This script is designed to scrape MMA event data from Tapology's fight center.
It collects event details, handles pagination, and avoids duplicates using SHA-256 hashes.
"""

# Import necessary libraries
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import time
import hashlib
import argparse

# Function to generate a SHA-256 hash of a string (normalized to lowercase, stripped)
def hash_string(input: str) -> str:
    """Hashes a string using SHA-256 and returns the hexadecimal digest."""
    normalized_input = input.lower().strip()
    hash_obj = hashlib.sha256(normalized_input.encode('utf-8'))
    return hash_obj.hexdigest()

class UFCScraper:
    def __init__(self, headless, wait_time, continous):
        """
        Initializes the scraper, sets up Chrome WebDriver, and attempts to load previous data.
        
        Args:
            headless (bool): Whether to run the browser in headless mode.
            wait_time (int): Time to wait after loading a page (in seconds).
        """
        self.base_url = "https://www.ufc.com/"
        self.wait_time = wait_time
        self.contious = continous
        options = webdriver.ChromeOptions()

        # Configure browser options for headless and stable operation
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--log-level=3")  # Minimize console output

        # Initialize the Chrome WebDriver
        self.driver = webdriver.Chrome(options=options)

        # Try to load previously scraped data
        try:
            self.data = pd.read_csv("Events.csv")
        except FileNotFoundError:
            self.data = None
        except pd.errors.EmptyDataError:
            self.data = None

        self.scraped_data = []  # Store results during this session

    def get_event_listing(self,page=0):
        listing_path = f"events?page={page}#events-list-past"
        self.driver.get(self.base_url + listing_path)
        time.sleep(self.wait_time)  # Wait for the page to load

        links = []

        card_selectors = self.driver.find_elements(By.CSS_SELECTOR,'div[style="visibility: inherit; opacity: 1; transform: matrix(1, 0, 0, 1, 0, 0);"]')
        for card_selector in card_selectors:
            try:
                element = card_selector.find_element(By.CSS_SELECTOR, 'a[href*="/event/"]')
                links.append(element.get_attribute('href'))
            except Exception as e:
                print(f"Error finding event link: {e}")
                if self.contious:
                    continue
                else:
                    break
        
        if not links:
            print(f"No links found on page {page}. Stopping pagination.")
            return
        
        print(f"Found {len(links)} links on page {page}.")
        return links
    
    def get_event_details(self, link):
        self.driver.get(link)
        time.sleep(self.wait_time)

        event_details = {}

        try:
            title_prefix = self.driver.find_element(By.CSS_SELECTOR, 'div.field--name-node-title h1').text.strip()
        except NoSuchElementException:
            title_prefix = None

        # Try to get top_name, vs_text, and bottom_name safely
        try:
            top_name = self.driver.find_element(By.CSS_SELECTOR, ".e-divider__top").text.strip()
        except NoSuchElementException:
            top_name = None

        try:
            vs_text = self.driver.find_element(By.CSS_SELECTOR, ".e-divider__border-text").text.strip()
        except NoSuchElementException:
            vs_text = None

        try:
            bottom_name = self.driver.find_element(By.CSS_SELECTOR, ".e-divider__bottom").text.strip()
        except NoSuchElementException:
            bottom_name = None

        # Build the final title based on what was found
        if top_name and vs_text and bottom_name:
            full_title = f"{title_prefix}: {top_name} {vs_text} {bottom_name}"
        else:
            full_title = title_prefix  # fallback to just the title prefix
        id = hash_string(full_title)

        if self.data is not None and id in self.data['id'].values:
            print(f"Event {full_title} already exists in the dataset. Skipping.")
            return
        else:
            event_details['id'] = id
            event_details['title'] = full_title
            event_details['link'] = link
        return event_details

    def run(self):
        running = True
        page = 0
        while running:
            try:
                links = self.get_event_listing(page)
                if links:
                    for link in links:
                        print(f"Processing link: {link}")
                        details = self.get_event_details(link)
                        if details:
                            self.scraped_data.append(details)
                        else:
                            if not self.contious:
                                running = False
                                break
                            else:
                                continue
                else:
                    running = False
            except Exception as e:
                print(f"Error on page {page}: {e}")
                running = False

            if running:
                page += 1

            self.save_data_to_csv()

    def save_data_to_csv(self):
        """
        Saves the scraped data to a CSV file.
        """
        if self.scraped_data:
            df = pd.DataFrame(self.scraped_data)
            if self.data is not None:
                df = pd.concat([df,self.data]).drop_duplicates(subset='id', keep='last')
            df.to_csv("Events.csv", index=False)
            print("Data saved to Events.csv")
        else:
            print("No new data to save.")

    def quit(self):
        """
        Saves the scraped data to CSV and shuts down the browser.
        """
        self.driver.quit()


# Entry point for script execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="FightIQ Scraper", description="Scrape UFC page for Event details, and fight details")

    # scraper arguements
    parser.add_argument("-H", "--headless", action="store_true", help="scrape in headless mode (do not display browser window)")
    parser.add_argument("-c","--continous", action="store_true", help="scrape even if previously scraped entries are present")
    parser.add_argument("-w","--wait", help="set maximum page load wait time", default=10)
    
    args = parser.parse_args()
    
    scraper = UFCScraper(headless=args.headless, wait_time=args.wait, continous=args.continous)
    try:
        scraper.run()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        scraper.quit()