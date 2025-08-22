import logging
from typing import Callable
from .base import BaseScraper
from exceptions import EntityExistsError

logger = logging.getLogger(__name__)


class UFCStatsScraper(BaseScraper):
    def __init__(self, wait_time: int, ignore_errors: bool):
        super().__init__(
            base_url="http://www.ufcstats.com/",
            wait_time=wait_time,
            ignore_errors=ignore_errors,
        )

        self.site_paths = {
            "event listing": "statistics/events/completed?page=",
            "events": "event-details/",
            "fights": "fight-details/",
        }

    ##########
    # FIGHTS #
    ##########

    def scrape_fight(self, id: str, event_id: str, early_stopping: Callable) -> dict|None:
        if early_stopping(id):
            return None

        url = self.base_url + self.site_paths["fights"] + id
        soup = self.fetch_soup(url)
        fighters = soup.select(".b-fight-details__person")

        fight_title = self.clean_text(
            self.parse_text(self.parse_element(soup, ".b-fight-details__fight-title"))
        )
        method = self.clean_text(
            self.parse_text(self.parse_element(soup, ".b-fight-details__text-item_first")).replace("Method:", "")
        )
        method = "DRAW" if method == "Other" else method

        fight_details = {
            "id": id,
            "event_id": event_id,
            "title": fight_title,
            "method": method,
        }

        fighter_details = {}
        for index, fighter in enumerate(fighters):
            fighter_type = "red" if index == 0 else "blue"
            name = self.clean_text(
                self.parse_text(self.parse_element(fighter, "h3.b-fight-details__person-name a"))
            )
            nickname = self.clean_text(
                self.parse_text(self.parse_element(fighter, "p.b-fight-details__person-title"))
            )
            result = self.clean_text(
                self.parse_text(self.parse_element(fighter, "i.b-fight-details__person-status"))
            )

            fighter_details |= {
                f"{fighter_type}_name": name,
                f"{fighter_type}_nickname": nickname,
                f"{fighter_type}_result": result,
            }

        fight_details |= fighter_details

        # Parse round and time
        info_items = self.parse_elements(soup, ".b-fight-details__text-item")
        round_, fight_time = "", ""
        for item in info_items[:3]:
            label = self.clean_text(
                self.parse_text(self.parse_element(item, ".b-fight-details__label"))
            )
            text = self.clean_text(self.parse_text(item).replace(label, ""))
            if label == "Round:":
                round_ = text
            elif label == "Time:":
                fight_time = text

        fight_details |= {"round": round_, "time": fight_time}

        return fight_details

    def scrape_fights(self, ids: list[str], event_id: str, early_stopping: Callable) -> list:
        data_collection = []
        for id in ids:
            data = self.scrape_fight(id,event_id,early_stopping)
            if data is None:
                return data_collection
            data_collection.append(data)
        return data_collection

    ##########
    # EVENTS #
    ##########

    def scrape_event_listing(self, page: int):
        url = self.base_url + self.site_paths["event listing"] + str(page)
        logger.debug(f"Fetching event listing page {page}: {url}")
        soup = self.fetch_soup(url)

        try:
            link_tags = self.parse_elements(soup, "tr.b-statistics__table-row a.b-link")
            ids = [self.parse_id_from_url(self.parse_Tag_attribute(link_tag, "href")) for link_tag in link_tags]
        except ValueError:
            # No events on this page → last page reached
            logger.info(f"No events found on page {page}. Ending pagination.")
            ids = []

        return ids

    def scrape_event(self, id: str, early_stopping: Callable) -> dict|None:
        if early_stopping(id):
            return 

        url = self.base_url + self.site_paths["events"] + id
        soup = self.fetch_soup(url)

        title = self.clean_text(self.parse_text(self.parse_element(soup, "h2.b-content__title")))
        date = self.clean_text(
            self.parse_text(self.parse_element(soup, "li.b-list__box-list-item:nth-child(1)")).replace("Date:", "")
        )
        location = self.clean_text(
            self.parse_text(self.parse_element(soup, "li.b-list__box-list-item:nth-child(2)")).replace("Location:", "")
        )

        fight_rows = self.parse_elements(soup, "tbody.b-fight-details__table-body tr.js-fight-details-click")
        fight_links = [self.parse_Tag_attribute(row, "data-link") for row in fight_rows]
        fight_ids = [self.parse_id_from_url(fight_link) for fight_link in fight_links]

        return {
            "id": id,
            "title": title,
            "date": date,
            "location": location,
            "fights": fight_ids,
        }

    def scrape_events(self, ids: list[str], early_stopping: Callable) -> list[dict]:
        data_collection = []
        for id in ids:
            data = self.scrape_event(id, early_stopping)
            if data is None:
                return data_collection
            data_collection.append(data)
        return data_collection
