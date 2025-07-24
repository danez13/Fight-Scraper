from .base import BaseScraper
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class UFCStatsScraper(BaseScraper):
    def __init__(self,headless:bool, wait_time:int, continuous:bool, direct:bool, pre_linked:bool, update:bool):
        super().__init__(base_url="http://www.ufcstats.com/", headless=headless, wait_time=wait_time, continuous=continuous, direct=direct, pre_linked=pre_linked, update=update)

    def get_event_listing_links(self,page) -> list[str]|None:
        self.driver.get(self.base_url + f"statistics/events/completed?page={page}")

        try:
            self.wait.until(EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "tr.b-statistics__table-row")
            ))
        except TimeoutException:
            print(f"Event cards did not load in time on page {page}")
            return

        rows = self.driver.find_elements(By.CSS_SELECTOR, "tr.b-statistics__table-row")
        links = []
        for row in rows:
            try:
                event_name_elem = row.find_element(By.CSS_SELECTOR, "a.b-link")
                event_url = event_name_elem.get_attribute("href")
                
                links.append(event_url)
            except Exception as e:
                # skip rows that don't have expected content
                continue

        return links
    
    def get_event_details(self,link) -> dict|None:
        self.driver.get(link)
        try:
            self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "section.b-statistics__section_details")))
        except TimeoutException:
            print(f"'{link}' did not load")
            return
        
        id = link.split("/")[-1]

        if self.current_events is not None and id in self.current_events['id'].values:
            print(f"Event already exists in the dataset. Skipping.")
            return

        event_title = self.driver.find_element(By.CSS_SELECTOR, "span.b-content__title-highlight").text.strip()
        event_date = self.driver.find_element(By.CSS_SELECTOR, "li.b-list__box-list-item:nth-child(1)").text.replace('DATE:', '').strip()
        event_location = self.driver.find_element(By.CSS_SELECTOR, "li.b-list__box-list-item:nth-child(2)").text.replace('LOCATION:', '').strip()

        rows = self.driver.find_elements(By.CSS_SELECTOR, "tbody.b-fight-details__table-body tr.js-fight-details-click")

        fight_links= []
        for row in rows:
            fight_link = row.get_attribute("data-link")
            fight_links.append(fight_link)

        fight_ids = []
        for fight_link in fight_links:
            fight_id = self.get_fight_details(fight_link,id)
            assert fight_id
            fight_ids.append(fight_id)

        return {
            "id":id,
            "title":event_title,
            "date":event_date,
            "location": event_location,
            "fights":fight_ids
        }

    def get_fight_fighter_details(self,type:str,content:WebElement) -> dict:
        name = content.find_element(By.CSS_SELECTOR, "h3.b-fight-details__person-name a").text.strip()
        nickname = content.find_element(By.CSS_SELECTOR, "p.b-fight-details__person-title").text.strip().replace('"',"")
        result = content.find_element(By.CSS_SELECTOR, "i.b-fight-details__person-status").text.strip()
        if result == "W":
            result = "WIN"
        elif result == "L":
            result = "LOSS"
        elif result == "D":
            result = "DRAW"
        else:
            result = "NO CONTEST"
        fighter_link = content.find_element(By.CSS_SELECTOR, "h3.b-fight-details__person-name a").get_attribute("href")
        return {
            f"{type}_name": name,
            f"{type}_nickname": nickname,
            f"{type}_result": result,
            f"{type}_link": fighter_link,
        }

    def get_fight_details(self,link,event_id) -> str|None:
        id = link.split("/")[-1]
        self.driver.get(link)
        
        try:
            self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "section.b-statistics__section_details")))
        except TimeoutException:
            print(f"'{link}' did not load")
            return
        
        fighters = self.driver.find_elements(By.CSS_SELECTOR, ".b-fight-details__person")
        fighters_details = dict()
        for index,fighter in enumerate(fighters):
            if index == 0:
                red_details = self.get_fight_fighter_details("red",fighter)
                fighters_details.update(red_details)
            else:
                blue_details = self.get_fight_fighter_details("blue",fighter)
                fighters_details.update(blue_details)
        fight_details = {"fight_id":id,"event_id":event_id,"link": link}
        self.new_fights.append(fight_details | fighters_details)
        
        return id

    def scrape_links(self,links:list):
        for link in links:
            print(f"Processing link: {link}")
            details = self.get_event_details(link)
            if details:
                self.new_events.append(details)
            else:
                if not self.continuous:
                    return False
                else:
                    continue
        return True

    def run(self):
        if self.pre_linked:
            if self.current_events is None:
                print("No current data loaded.")
                return

            links = list(self.current_events["link"])

            self.scrape_links(links)
        else:
            running = True
            page = 1

            while running:
                try:
                    links = self.get_event_listing_links(page)

                    if links:
                        running = self.scrape_links(links)
                    else:
                        running = False
                except Exception as e:
                    raise e
                
                if running:
                    page +=1
                self.save(self.direct)