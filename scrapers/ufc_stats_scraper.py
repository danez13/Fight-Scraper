import logging
from .base import BaseScraper
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

logger = logging.getLogger(__name__)

class UFCStatsScraper(BaseScraper):
    def __init__(self, headless: bool, wait_time: int, continuous: bool, direct: bool, pre_linked: bool, update: bool):
        super().__init__(
            base_url="http://www.ufcstats.com/",
            headless=headless,
            wait_time=wait_time,
            continuous=continuous,
            direct=direct,
            pre_linked=pre_linked,
            update=update
        )

    def get_event_listing_links(self, page) -> list[str] | None:
        logger.debug(f"Fetching event listing page {page}")
        self.driver.get(self.base_url + f"statistics/events/completed?page={page}")

        try:
            self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.b-statistics__table-row")))
        except TimeoutException:
            logger.warning(f"Event cards did not load in time on page {page}")
            return None

        rows = self.driver.find_elements(By.CSS_SELECTOR, "tr.b-statistics__table-row")
        links = []
        for row in rows:
            try:
                event_name_elem = row.find_element(By.CSS_SELECTOR, "a.b-link")
                event_url = event_name_elem.get_attribute("href")
                links.append(event_url)
            except Exception as e:
                logger.debug("Skipping row without expected content: %s", e)
                continue

        logger.info(f"Found {len(links)} events on page {page}")
        return links

    def get_event_details(self, link) -> dict | None:
        logger.info(f"Scraping event details from {link}")
        self.driver.get(link)

        try:
            self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "section.b-statistics__section_details")))
        except TimeoutException:
            logger.warning(f"Event page '{link}' did not load in time.")
            return None

        event_id = link.split("/")[-1]

        if self.current_events is not None and event_id in self.current_events["id"].values and not self.update:
            logger.info(f"Event {event_id} already exists. Skipping.")
            return None

        try:
            event_title = self.driver.find_element(By.CSS_SELECTOR, "span.b-content__title-highlight").text.strip()
            event_date = self.driver.find_element(By.CSS_SELECTOR, "li.b-list__box-list-item:nth-child(1)").text.replace("DATE:", "").strip()
            event_location = self.driver.find_element(By.CSS_SELECTOR, "li.b-list__box-list-item:nth-child(2)").text.replace("LOCATION:", "").strip()
        except Exception as e:
            logger.error(f"Failed to parse basic event details from {link}: {e}")
            return None

        rows = self.driver.find_elements(By.CSS_SELECTOR, "tbody.b-fight-details__table-body tr.js-fight-details-click")
        fight_links = [row.get_attribute("data-link") for row in rows]

        fight_ids = []
        for fight_link in fight_links:
            logger.info(f"Processing fight link: {fight_link}")
            fight_id = self.get_fight_details(fight_link, event_id)
            if fight_id:
                fight_ids.append(fight_id)
            else:
                logger.warning(f"Failed to scrape fight: {fight_link}")

        logger.info(f"Scraped event {event_title} with {len(fight_ids)} fights")
        return {
            "id": event_id,
            "title": event_title,
            "date": event_date,
            "location": event_location,
            "fights": fight_ids
        }

    def get_fight_fighter_details(self, type: str, content: WebElement) -> dict:
        name = content.find_element(By.CSS_SELECTOR, "h3.b-fight-details__person-name a").text.strip()
        nickname = content.find_element(By.CSS_SELECTOR, "p.b-fight-details__person-title").text.strip().replace('"', "")
        result = content.find_element(By.CSS_SELECTOR, "i.b-fight-details__person-status").text.strip()

        result_map = {"W": "WIN", "L": "LOSS", "D": "DRAW"}
        result = result_map.get(result, "NO CONTEST")

        fighter_link = content.find_element(By.CSS_SELECTOR, "h3.b-fight-details__person-name a").get_attribute("href")

        return {
            f"{type}_name": name,
            f"{type}_nickname": nickname,
            f"{type}_result": result,
            f"{type}_link": fighter_link,
        }

    def get_fight_details(self, link, event_id) -> str | None:
        fight_id = link.split("/")[-1]
        logger.debug(f"Scraping fight {fight_id} from {link}")
        self.driver.get(link)

        try:
            self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "section.b-statistics__section_details")))
        except TimeoutException:
            logger.warning(f"Fight page '{link}' did not load.")
            return None

        fighters = self.driver.find_elements(By.CSS_SELECTOR, ".b-fight-details__person")
        fighters_details = {}
        for index, fighter in enumerate(fighters):
            fighter_type = "red" if index == 0 else "blue"
            details = self.get_fight_fighter_details(fighter_type, fighter)
            fighters_details.update(details)
        
        bout_type = self.driver.find_element(By.CSS_SELECTOR, ".b-fight-details__fight-title").text.strip()
        bout_type = bout_type.replace("BOUT", "").strip()

        # Get method, round, time, referee
        method = self.driver.find_element(By.CSS_SELECTOR, ".b-fight-details__text-item_first").text.strip()
        if method == "Other":
            method = "DRAW"
        info_items = self.driver.find_elements(By.CSS_SELECTOR, ".b-fight-details__text-item")
        round_ = fight_time = total_rounds = total_time = ""
        judges = []

        for item in info_items[:3]:
            label = item.find_element(By.CSS_SELECTOR, ".b-fight-details__label").text.strip()
            text = item.text.replace(label, '').strip()

            if label == "ROUND:":
                round_ = text
            elif label == "TIME:":
                fight_time = text
            elif label == "TIME FORMAT:":

                if text == "No Time Limit":
                    total_rounds = "1"
                    total_time = "No Time Limit"
                elif text.startswith("1 Rnd +"):
                    total_rounds = "1"
                    raw_time = text.split("(")
                else:
                    format = text.split(" ")
                    total_rounds = format[0]
                    total_time = sum(int(round_time.strip("()")) for round_time in format[-1].split("-"))


        fight_details = {"fight_id": fight_id, "event_id": event_id, "link": link, "weight": bout_type,
                         "method": method, "round": round_, "time": fight_time, "total_rounds": total_rounds,
                         "total_time": total_time}
        self.new_fights.append(fight_details | fighters_details)

        logger.info(f"Scraped fight {fight_id} for event {event_id}")
        return fight_id

    def scrape_links(self, links: list):
        for link in links:
            logger.info(f"Processing event link: {link}")
            details = self.get_event_details(link)
            if details:
                self.new_events.append(details)
            elif not self.continuous:
                logger.warning("Encountered failure and not in continuous mode — stopping.")
                return False
        return True

    def run(self):
        logger.info("UFCStatsScraper started.")
        if self.pre_linked:
            if self.current_events is None:
                logger.warning("No current events loaded — cannot proceed with pre_linked.")
                return

            links = list(self.current_events["link"])
            self.scrape_links(links)
        else:
            running = True
            page = 1
            while running:
                try:
                    logger.debug(f"Scraping event page {page}")
                    links = self.get_event_listing_links(page)
                    if links:
                        running = self.scrape_links(links)
                    else:
                        logger.info("No more links found — stopping.")
                        running = False
                except Exception as e:
                    logger.exception(f"Unexpected error on page {page}: {e}")
                    raise e

                if running:
                    page += 1
                self.save(self.direct)
        logger.info("UFCStatsScraper finished.")
