import logging
from bs4 import BeautifulSoup
from .base import BaseScraper

logger = logging.getLogger(__name__)

class UFCStatsScraper(BaseScraper):
    def __init__(self, wait_time: int, continuous: bool, direct: bool, pre_linked: bool, update: bool):
        super().__init__(
            base_url="http://www.ufcstats.com/",
            wait_time=wait_time,
            continuous=continuous,
            direct=direct,
            pre_linked=pre_linked,
            update=update
        )

    def get_event_listing_links(self, page: int) -> list[str] | None:
        url = self.base_url + f"statistics/events/completed?page={page}"
        logger.debug(f"Fetching event listing page {page}: {url}")
        soup = self.fetch_soup(url)
        if soup is None:
            return None

        rows = soup.select("tr.b-statistics__table-row")
        links = []
        for row in rows:
            link_tag = row.select_one("a.b-link")
            if link_tag and link_tag.get("href"):
                links.append(link_tag["href"])
        logger.info(f"Found {len(links)} events on page {page}")
        return links

    def get_event_details(self, link: str) -> dict | None:
        logger.info(f"Scraping event details from {link}")
        soup = self.fetch_soup(link)
        if soup is None:
            return None

        event_id = link.split("/")[-1]

        if self.current_events is not None and event_id in self.current_events["id"].values and not self.update:
            logger.info(f"Event {event_id} already exists. Skipping.")
            return None

        try:
            title_elem = soup.select_one("span.b-content__title-highlight")
            date_elem = soup.select_one("li.b-list__box-list-item:nth-child(1)")
            location_elem = soup.select_one("li.b-list__box-list-item:nth-child(2)")

            event_title = title_elem.text.strip() if title_elem else ""
            event_date = date_elem.text.replace("DATE:", "").strip() if date_elem else ""
            event_location = location_elem.text.replace("LOCATION:", "").strip() if location_elem else ""
        except Exception as e:
            logger.error(f"Failed to parse basic event details from {link}: {e}")
            return None

        fight_rows = soup.select("tbody.b-fight-details__table-body tr.js-fight-details-click")
        fight_links = [row.get("data-link") for row in fight_rows if row.get("data-link")]

        fight_ids = []
        for fight_link in fight_links:
            logger.info(f"Processing fight link: {fight_link}")
            if fight_link is not None:
                fight_id = self.get_fight_details(str(fight_link), event_id)
            else:
                fight_id = None
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

    from bs4 import Tag

    def get_fight_fighter_details(self, type: str, content: Tag) -> dict:
        try:
            name_elem = content.select_one("h3.b-fight-details__person-name a")
            name = name_elem.text.strip() if name_elem else ""
            nickname_elem = content.select_one("p.b-fight-details__person-title")
            nickname = nickname_elem.text.strip().replace('"', "") if nickname_elem else ""
            result_elem = content.select_one("i.b-fight-details__person-status")
            result = result_elem.text.strip() if result_elem else "NO CONTEST"

            result_map = {"W": "WIN", "L": "LOSS", "D": "DRAW"}
            result = result_map.get(result, "NO CONTEST")

            fighter_link_elem = content.select_one("h3.b-fight-details__person-name a")
            fighter_link = fighter_link_elem.get("href") if fighter_link_elem else ""

            return {
                f"{type}_name": name,
                f"{type}_nickname": nickname,
                f"{type}_result": result,
                f"{type}_link": fighter_link,
            }
        except Exception as e:
            logger.warning(f"Failed to extract fighter ({type}) details: {e}")
            return {
                f"{type}_name": "",
                f"{type}_nickname": "",
                f"{type}_result": "NO CONTEST",
                f"{type}_link": ""
            }

    def get_fight_details(self, link: str, event_id: str) -> str | None:
        fight_id = link.split("/")[-1]
        logger.debug(f"Scraping fight {fight_id} from {link}")
        soup = self.fetch_soup(link)
        if soup is None:
            return None

        fighters = soup.select(".b-fight-details__person")
        fighters_details = {}
        for index, fighter in enumerate(fighters):
            fighter_type = "red" if index == 0 else "blue"
            details = self.get_fight_fighter_details(fighter_type, fighter)
            fighters_details.update(details)

        bout_elem = soup.select_one(".b-fight-details__fight-title")
        bout_type = bout_elem.text.replace("BOUT", "").strip() if bout_elem else ""

        method_elem = soup.select_one(".b-fight-details__text-item_first")
        method = method_elem.text.strip() if method_elem else ""
        if method == "Other":
            method = "DRAW"

        info_items = soup.select(".b-fight-details__text-item")
        round_, fight_time, total_rounds, total_time = "", "", "", ""

        for item in info_items[:3]:
            label_elem = item.select_one(".b-fight-details__label")
            if not label_elem:
                continue
            label = label_elem.text.strip()
            text = item.text.replace(label, "").strip()

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
                    total_time = "OT"
                else:
                    try:
                        format_parts = text.split(" ")
                        total_rounds = format_parts[0]
                        time_per_round = format_parts[-1].replace("(", "").replace(")", "")
                        minutes, seconds = map(int, time_per_round.split("-"))
                        total_time = str(int(total_rounds) * (minutes * 60 + seconds))
                    except Exception as e:
                        logger.warning(f"Could not parse time format: {text} — {e}")
                        total_time = ""

        fight_details = {
            "fight_id": fight_id,
            "event_id": event_id,
            "link": link,
            "weight": bout_type,
            "method": method,
            "round": round_,
            "time": fight_time,
            "total_rounds": total_rounds,
            "total_time": total_time
        }

        self.new_fights.append(fight_details | fighters_details)
        logger.info(f"Scraped fight {fight_id} for event {event_id}")
        return fight_id

    def scrape_links(self, links: list) -> bool:
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
