import logging
from .base import BaseScraper
from exceptions import EntityExistsError
from datasets import Dataset
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class UFCStatsScraper(BaseScraper):
    def __init__(self, wait_time: int, ignore_errors: bool, direct: bool, update: bool):
        super().__init__(
            base_url="http://www.ufcstats.com/",
            wait_time=wait_time,
            ignore_errors=ignore_errors,
            direct=direct,
            update=update
        )
        self.events_dataset = Dataset("Events",["id", "title", "date", "location", "fights"])
        self.fights_dataset = Dataset("Fights",["id", "event_id", "weight", "method", "round", "time",
                                                    "red_name", "red_nickname", "red_result",
                                                    "blue_name", "blue_nickname", "blue_result"])

    def scrape_event_listings(self, page: int = 1) -> list[str]: 
        try:
            url = self.base_url + self.url_paths["events listing"] + str(page)
            logger.debug(f"Fetching event listing page {page}: {url}")
            soup = self.fetch_soup(url)
            link_tags = self.parse_elements(soup, "tr.b-statistics__table-row a.b-link")

            links = [self.parse_Tag_attribute(link_tag, "href") for link_tag in link_tags]
            return [self.parse_id_from_url(link) for link in links]

        except Exception as e:
            logger.error(f"Failed to scrape event listings on page {page}: {e}")
            raise e
        
    def parse_fight_listing(self,soup:BeautifulSoup) -> list[str]:
        fight_rows = self.parse_elements(soup, "tbody.b-fight-details__table-body tr.js-fight-details-click")
        fight_links = [self.parse_Tag_attribute(row, "data-link") for row in fight_rows]

        return [self.parse_id_from_url(fight_link) for fight_link in fight_links]
    
    def scrape_events(self,page:int)->bool:
        event_ids = self.scrape_event_listings(page)
        for event_id in event_ids:
            self.scrape_event(event_id)
            self.events_dataset.save(self.direct)
            if event_id == "6420efac0578988b":
                return False

        return True
        
        
    def scrape_event(self, id:str):
        if self.events_dataset.does_id_exist(id,to_buffer=False) and not self.update:
            raise EntityExistsError("Event", id)

        url = self.base_url + self.url_paths["events"] + id
        soup = self.fetch_soup(url)
        
        title = self.clean_text(self.parse_text(self.parse_element(soup,"h2.b-content__title")))
        date = self.clean_text(self.parse_text(self.parse_element(soup,"li.b-list__box-list-item:nth-child(1)")).replace("Date:", ""))
        location = self.clean_text(self.parse_text(self.parse_element(soup,"li.b-list__box-list-item:nth-child(2)")).replace("Location:", ""))

        fight_ids = self.parse_fight_listing(soup)

        if self.update:
            self.events_dataset.update_row(id, {
                "id": id,
                "title": title,
                "date": date,
                "location": location,
                "fights": fight_ids
            })
        else:
            self.events_dataset.add_row({
                "id": id,
                "title": title,
                "date": date,
                "location": location,
                "fights": fight_ids
            })

    def scrape_fights(self,page:int) -> bool:
        event_ids = self.scrape_event_listings(page)
        for event_id in event_ids:
            url = self.base_url + self.url_paths["events"] + event_id

            soup = self.fetch_soup(url)

            fight_ids = self.parse_fight_listing(soup)
            for fight_id in fight_ids:
                self.scrape_fight(fight_id,event_id)
                self.fights_dataset.save(self.direct)

            if event_id == "6420efac0578988b":
                return False
        return True

    def scrape_fight(self, id:str, event_id:str):
        if self.fights_dataset.does_id_exist(id) and not self.update:
            raise EntityExistsError("Fight", id)
        url = self.base_url + self.url_paths["fights"] + id
        soup = self.fetch_soup(url)
        fighters = soup.select(".b-fight-details__person")

        weight = self.clean_text(self.parse_text(self.parse_element(soup, ".b-fight-details__fight-title")).replace("Bout",""))
        method = self.clean_text(self.parse_text(self.parse_element(soup, ".b-fight-details__text-item_first")).replace("Method:",""))
        method = "DRAW" if method == "Other" else method

        fight_details = {
            "id": id,
            "event_id": event_id,
            "weight": weight,
            "method": method
        }

        fighter_details = {}
        for index, fighter in enumerate(fighters):
            fighter_type = "red" if index == 0 else "blue"
            name = self.clean_text(self.parse_text(self.parse_element(fighter, "h3.b-fight-details__person-name a")))
            nickname = self.clean_text(self.parse_text(self.parse_element(fighter,"p.b-fight-details__person-title")))
            result = self.clean_text(self.parse_text(self.parse_element(fighter,"i.b-fight-details__person-status")))

            fighter_details = fighter_details | {
                f"{fighter_type}_name": name,
                f"{fighter_type}_nickname": nickname,
                f"{fighter_type}_result": result
            }

        fight_details = fight_details | fighter_details
        
        info_items = self.parse_elements(soup,".b-fight-details__text-item")
        round_, fight_time = "", ""

        for item in info_items[:3]:
            label = self.clean_text(self.parse_text(self.parse_element(item,".b-fight-details__label")))

            text = self.clean_text(self.parse_text(item).replace(label, ""))
            if label == "Round:":
                round_ = text
            elif label == "Time:":
                fight_time = text

        fight_details = fight_details | {
            "round": round_,
            "time": fight_time
        }
        
        if self.update:
            self.fights_dataset.update_row(id, fight_details)
        else:
            self.fights_dataset.add_row(fight_details)
            
    def scrape_all(self, page:int) -> bool:
        event_ids = self.scrape_event_listings(page)

        for event_id in event_ids:
            self.scrape_event(event_id)
            self.events_dataset.save(self.direct)

            fight_ids = self.events_dataset.get_instance_column(from_buffer=False,id=event_id,column="fights")
            for fight_id in fight_ids:
                self.scrape_fight(fight_id, event_id)
                self.fights_dataset.save(direct=self.direct)
            
            if event_id == "6420efac0578988b":
                return False
        return True

    def quit(self, error: bool):
        """Handle cleanup and saving on exit."""
        if error:
            logger.warning("Exiting with errors — saving to temp files")
            self.events_dataset.save(direct=False)
            self.fights_dataset.save(direct=False)
        else:
            logger.info("Exiting without errors — saving to final files")
            self.events_dataset.save(direct=True)
            self.fights_dataset.save(direct=True)