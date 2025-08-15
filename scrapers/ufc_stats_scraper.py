import logging
from .base import BaseScraper
from exceptions import EntityExistsError
from datasets import Dataset

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
        self.fights_dataset = Dataset("Fights",["id", "event_id", "title", "method", "round", "time",
                                                    "red_name", "red_nickname", "red_result",
                                                    "blue_name", "blue_nickname", "blue_result"])

    def run(self):
        logger.info("UFCStatsScraper started.")
        page = 1
        running = True
        while running:
            url = self.base_url + f"statistics/events/completed?page={page}"
            logger.debug(f"Fetching event listing page {page}: {url}")
            
            try:
                soup = self.fetch_soup(url)
                link_tags = self.parse_elements(soup, "tr.b-statistics__table-row a.b-link")

                links = [self.parse_Tag_attribute(link_tag, "href") for link_tag in link_tags]

                for link in links:
                    event_id = self.parse_id_from_url(link)

                    if self.events_dataset.does_id_exist(event_id) and not self.update:
                        raise EntityExistsError("Event", event_id)
                    
                    soup = self.fetch_soup(link)
                    
                    title = self.clean_text(self.parse_text(self.parse_element(soup,"h2.b-content__title")))
                    date = self.clean_text(self.parse_text(self.parse_element(soup,"li.b-list__box-list-item:nth-child(1)")).replace("Date:", ""))
                    location = self.clean_text(self.parse_text(self.parse_element(soup,"li.b-list__box-list-item:nth-child(2)")).replace("Location:", ""))

                    fight_rows = self.parse_elements(soup, "tbody.b-fight-details__table-body tr.js-fight-details-click")
                    fight_links = [self.parse_Tag_attribute(row, "data-link") for row in fight_rows]

                    fight_ids = [self.parse_id_from_url(fight_link) for fight_link in fight_links]


                    if self.update:
                        self.events_dataset.update_row(event_id, {
                            "id": event_id,
                            "title": title,
                            "date": date,
                            "location": location,
                            "fights": fight_ids
                        })
                    else:
                        self.events_dataset.add_row({
                            "id": event_id,
                            "title": title,
                            "date": date,
                            "location": location,
                            "fights": fight_ids
                        })
                    self.events_dataset.save(direct=self.direct)

                    for fight_id, fight_link in zip(fight_ids,fight_links):
                        
                        if self.fights_dataset.does_id_exist(fight_id) and not self.update:
                            raise EntityExistsError("Fight", fight_id)
                        
                        soup = self.fetch_soup(fight_link)
                        fighters = soup.select(".b-fight-details__person")

                        fight_title = self.clean_text(self.parse_text(self.parse_element(soup, ".b-fight-details__fight-title")))
                        method = self.clean_text(self.parse_text(self.parse_element(soup, ".b-fight-details__text-item_first")).replace("Method:",""))
                        method = "DRAW" if method == "Other" else method

                        fight_details = {
                            "id": fight_id,
                            "event_id": event_id,
                            "title": fight_title,
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
                            self.fights_dataset.update_row(fight_id, fight_details)
                        else:
                            self.fights_dataset.add_row(fight_details)

                        self.fights_dataset.save(direct=self.direct)
                    if event_id == "6420efac0578988b":
                        running = False
                        break
                        
            except EntityExistsError as e:
                logger.warning(f"Entity already exists: {e}")
                break
            except Exception as e:
                if self.ignore_errors:
                    logger.error(f"Error on page {page}: {e}")
                    page += 1
                    continue
                else:
                    logger.exception("An error occurred during scraping.")
                    raise e
            page += 1
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