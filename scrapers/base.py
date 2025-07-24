# Import necessary libraries
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import hashlib
import pandas as pd
import os
from abc import ABC, abstractmethod
class BaseScraper(ABC):
    def __init__(self,base_url:str, headless:bool, wait_time:int, continuous:bool, direct:bool, pre_linked:bool, update:bool):
        self.base_url = base_url
        self.continuous = continuous
        self.direct = direct
        self.pre_linked = pre_linked
        self.update = update
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

        self.wait = WebDriverWait(self.driver, wait_time)  # wait up to 10 seconds

        # Attempt to load or initialize event and fight data
        self.current_events = self.initialize_data("Events.csv",["id", "title", "date", "location", "fights"])
        self.current_fights = self.initialize_data("Fights.csv", ["fight_id","event_id","link", "red_name","red_nickname","red_result","red_link", "blue_name","blue_nickname","blue_result","blue_link"])


        self.new_events = []
        self.new_fights = []
    
    def initialize_data(self,filename:str, columns:list):
        try:
            return pd.read_csv(filename)
        except:
            return pd.DataFrame(columns=columns)
        
    @abstractmethod
    def run(self):
        pass
        
    def save_data_to_csv(self,new_data,current_data,filename,subset):
        if new_data:
            df = pd.DataFrame(new_data)

            if current_data is not None and not self.update:
                df = pd.concat([current_data,df]).drop_duplicates(subset=subset,keep="last")

            df.to_csv(filename,index=False)

            print(f"Data saved to {filename}")
        else:
            print("no new data to save.")
        
        new_data = []

    def save(self,direct:bool):
        if not direct:
            self.save_data_to_csv(self.new_events,self.current_events,"temp_events.csv","id")
            self.save_data_to_csv(self.new_fights,self.current_fights,"temp_fights.csv","fight_id")
        else:
            self.save_data_to_csv(self.new_events,self.current_events,"Events.csv","id")
            self.save_data_to_csv(self.new_fights,self.current_fights,"Fights.csv","fight_id")

    def quit(self, error:bool):
        if error:
            self.save(direct=True)
        else:
            self.save(direct=False)
            self.remove_existing_file("temp_events.csv")
            self.remove_existing_file("temp_fights.csv")

    def remove_existing_file(self,filename):
        if os.path.exists(filename):
            os.remove(filename)
            print(f"File {filename} deleted successfully")
        else:
            print(f"File {filename} does not exist.")