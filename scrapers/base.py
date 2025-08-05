# Import necessary libraries
import os
import logging
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import pandas as pd

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    def __init__(self, base_url: str, headless: bool, wait_time: int, continuous: bool, direct: bool, pre_linked: bool, update: bool):
        self.base_url = base_url
        self.continuous = continuous
        self.direct = direct
        self.pre_linked = pre_linked
        self.update = update

        options = webdriver.ChromeOptions()

        # Configure browser options
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--log-level=3")  # Silence ChromeDriver output

        logger.debug("Initializing Chrome WebDriver")
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, wait_time)

        # Load or initialize data
        self.current_events = self.initialize_data("Events.csv", ["id", "title", "date", "location", "fights"])
        self.current_fights = self.initialize_data("Fights.csv", ["fight_id", "event_id", "link", "weight","method","round","time","total_rounds","total_time", "red_name", "red_nickname", "red_result", "red_link", "blue_name", "blue_nickname", "blue_result", "blue_link"])

        self.new_events = []
        self.new_fights = []

    def initialize_data(self, filename: str, columns: list):
        try:
            logger.debug(f"Attempting to load {filename}")
            return pd.read_csv(filename)
        except Exception as e:
            logger.warning(f"Could not load {filename}, initializing empty DataFrame. Reason: {e}")
            return pd.DataFrame(columns=columns)

    @abstractmethod
    def run(self):
        pass

    def save_data_to_csv(self, new_data, current_data, filename, subset):
        if new_data:
            df = pd.DataFrame(new_data)

            if current_data is not None and not self.update:
                df = pd.concat([df,current_data]).drop_duplicates(subset=subset, keep="last")
            elif current_data is not None:
                df = pd.concat([current_data,df]).drop_duplicates(subset=subset, keep="last")

            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
        else:
            logger.info(f"No new data to save to {filename}")

    def save(self, direct: bool):
        if not direct:
            logger.info("Saving to temp files (non-direct mode)")
            self.save_data_to_csv(self.new_events, self.current_events, "temp_events.csv", "id")
            self.save_data_to_csv(self.new_fights, self.current_fights, "temp_fights.csv", "fight_id")
        else:
            logger.info("Saving to final CSVs (direct mode)")
            self.save_data_to_csv(self.new_events, self.current_events, "Events.csv", "id")
            self.save_data_to_csv(self.new_fights, self.current_fights, "Fights.csv", "fight_id")

    def quit(self, error: bool):
        if error:
            logger.warning("Exiting with errors — saving to temp files")
            self.save(direct=False)
        else:
            logger.info("Exiting cleanly — saving to final files and cleaning up")
            self.save(direct=True)
            self.remove_existing_file("temp_events.csv")
            self.remove_existing_file("temp_fights.csv")

        logger.debug("Closing browser")
        self.driver.quit()

    def remove_existing_file(self, filename):
        if os.path.exists(filename):
            os.remove(filename)
            logger.info(f"File {filename} deleted successfully")
        else:
            logger.debug(f"File {filename} does not exist — nothing to delete")
