import logging
from collections import OrderedDict
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
            "fighter listing": "statistics/fighters?",
            "fighters": "fighter-details/"
        }

    ############
    # FIGHTERS #
    ############

    def scraper_fighter_listing(self,char:str,page:int):
        url = self.base_url + self.site_paths["fighter listing"] + "char=" + char + "&page=" + str(page)
        logger.debug(f"Fetching fighter listing character {char} page {page}: {url}")
        soup = self.fetch_soup(url)
        try:
            link_tags = self.parse_elements(soup,"tr.b-statistics__table-row td.b-statistics__table-col a.b-link")
            ids = list(OrderedDict.fromkeys([self.parse_id_from_url(self.parse_Tag_attribute(link_tag, "href")) for link_tag in link_tags]))
        except:
            # No events on this page → last page reached
            logger.info(f"No fighter found on character {char} page {page}. Ending pagination.")
            ids = []
        return ids
    
    def scrape_fighter(self,id:str):
        url = self.base_url + self.site_paths["fighters"] + id
        soup = self.fetch_soup(url)
        name = self.clean_text(self.parse_text(self.parse_element(soup,"h2.b-content__title span.b-content__title-highlight")))
        record = self.clean_text(self.parse_text(self.parse_element(soup,"h2.b-content__title span.b-content__title-record")).replace("Record:",""))
        win,loss,draw = record.split("-")
        split = draw.split(" ",1)
        
        if len(split) == 2:
            draw,nc = split
            nc = self.clean_text(nc.replace(" NC",""))
        else:
            draw = draw
            nc = "0"
        bio_items = self.parse_elements(soup,"div.b-list__info-box ul.b-list__box-list li")

        bio = {}
        for item in bio_items:
            label = self.parse_element(item,"i")
            if label:
                key = self.clean_text(self.parse_text(label).replace(":",""))
                if key == "":
                    continue
                value = self.clean_text(self.parse_text(item).replace(self.clean_text(self.parse_text(label)),""))
                bio[key.lower()] = value
        try:
            fight_rows = self.parse_elements(soup,"table.b-fight-details__table tbody tr.b-fight-details__table-row__hover")
            fights = []
            for row in fight_rows:
                cols = self.parse_elements(row,"td")
                result = self.clean_text(self.parse_text(cols[0]))
                opponent = self.parse_id_from_url(self.parse_Tag_attribute(self.parse_elements(cols[1],"a")[1],"href"))
                fight_id = self.parse_id_from_url(self.parse_Tag_attribute(row,"data-link"))
                fights.append({
                    "fight": fight_id,
                    "fighter":id,
                    "opponent":opponent,
                    "result":result,
                    })
        except:
            fights = []

        return {
            "id":id,
            "name":name,
            "win":win,
            "loss":loss,
            "draw":draw,
            "no contest": nc,
            "fights":fights
        } | bio
    def scrape_fighters(self,ids,early_stopping:Callable):
        data_collection = []
        for id in ids:
            if early_stopping(id):
                return data_collection
            data = self.scrape_fighter(id)
            data_collection.append(data)
        return data_collection
    
    ##########
    # FIGHTS #
    ##########

    def scrape_fight(self, id: str, event_id: str) -> dict:

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
            "event": event_id,
            "title": fight_title,
            "method": method,
        }

        fighter_details = {}
        for index, fighter in enumerate(fighters):
            fighter_type = "red" if index == 0 else "blue"
            fighter_id = self.parse_id_from_url(self.parse_Tag_attribute(self.parse_element(fighter, "h3.b-fight-details__person-name a"),"href"))
            fighter_details |= {
                f"{fighter_type}_id": fighter_id,
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
            if early_stopping(id):
                return data_collection
            data = self.scrape_fight(id,event_id)
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

    def scrape_event(self, id: str) -> dict:
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
        fight_ids = []
        fight_weights = []
        for row in fight_rows:
            fight_id = self.parse_id_from_url(self.parse_Tag_attribute(row, "data-link"))
            fight_ids.append(fight_id)
            weight = self.clean_text(self.parse_text(self.parse_element(row,"td.b-fight-details__table-col.l-page_align_left:nth-of-type(7)")))
            fight_weights.append(weight)
        return {
            "id": id,
            "title": title,
            "date": date,
            "location": location,
            "fights": fight_ids,
            "weights":fight_weights
        }
    
    def scrape_events(self, ids: list[str], early_stopping: Callable) -> list[dict]:
        data_collection = []
        for id in ids:
            if early_stopping(id):
                return data_collection
            data = self.scrape_event(id)
            data_collection.append(data)
        return data_collection
