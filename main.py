import argparse
import logging
from typing import Callable
from exceptions import EntityExistsError
from scrapers import UFCStatsScraper
from datasets import Dataset, DataController
from logging_config import setup_logging

logger = logging.getLogger(__name__)


def attempt_func(func:Callable, args, ignore_errors: bool):
    results = []
    try:
        results =  func(**args)
        return results
    except EntityExistsError as e:
        if ignore_errors:
            logger.warning(f"Ignored existing entity: {e}")
        return results
    except Exception as e:
        if ignore_errors:
            logger.warning(f"Ignored error: {e}")
            return []
        else:
            raise

############
# Fighters #
############

def fighter_listing_scraping(scraper:UFCStatsScraper,char:str,page:int):
    return scraper.run(scraper.scraper_fighter_listing,parameters={"char":char,"page":page})

def fighter_scraping(scraper:UFCStatsScraper,ids:list,early_stopping:Callable):
    return scraper.run(
        scraper.scrape_fighters,
        parameters={"ids":ids,"early_stopping":early_stopping}
    )
##########
# Fights #
##########

def fight_scraping(scraper: UFCStatsScraper, events: list[dict], early_stopping):
    data_collection = []
    for event in events:
        fights = scraper.run(
            scraper.scrape_fights,
            parameters={
                "ids": event["fights"],
                "event_id": event["id"],
                "early_stopping": early_stopping,
            },
        )
        for fight,weight in zip(fights,event["weights"]):
            fight["event"] = event["id"]
            fight["weight"] = weight
        data_collection.extend(fights)
    return data_collection

##########
# Events #
##########

def event_scraping(scraper:UFCStatsScraper,ids:list[str],early_stopping:Callable):
    return scraper.run(
                scraper.scrape_events,
                parameters={"ids": ids, "early_stopping": early_stopping},
            )

def event_listing_scraping(scraper:UFCStatsScraper,page:int):
    return scraper.run(scraper.scrape_event_listing,{"page":page})

########
# Main #
########

def main(cli_args=None, log=True) -> int:
    if log:
        setup_logging(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="UFC Stats Scraper")
    parser.add_argument("-i", "--ignore", 
                        action="store_true", 
                        help="Ignore errors and continue scraping")
    parser.add_argument("-d", "--direct", 
                        action="store_true", 
                        help="Fetch requests directly (no retries)")
    parser.add_argument("-u", "--update", 
                        action="store_true", 
                        help="Update previously scraped data")
    parser.add_argument("-e", "--no-events", 
                        action="store_true", 
                        help="Do not scraper events")
    parser.add_argument("-f", "--no-fights", 
                        action="store_true", 
                        help="Do not scrape fights")
    parser.add_argument("-fi", "--no-fighters", 
                        action="store_true", 
                        help="Do not scrape fighters")
    parser.add_argument("-p","--prepend",
                        action="store_true",
                        help="Save intermediate results to temporary files during scraping")
    parser.add_argument("-w","--wait",
                        default=10,
                        type=int,
                        help="set wait time")
    args = parser.parse_args(cli_args)

    scraper = UFCStatsScraper(wait_time=args.wait, ignore_errors=args.ignore)

    # Initialize datasets
    controller = DataController(["events","fights","fighters","fighter_fights"],args.update,args.direct)

    fights_scraping_initializer = []
    if not args.no_events:
        page = 1
        while True:
            event_ids = attempt_func(event_listing_scraping,{"scraper":scraper,"page":page},args.ignore)

            if len(event_ids) == 0:
                logger.info(f"No more events found after page {page}.")
                break

            events_page_data = attempt_func(event_scraping,{"scraper":scraper,"ids":event_ids,"early_stopping":controller.get_early_stopping("events")},args.ignore)
            controller.insert("events",events_page_data,args.prepend)
            logger.info(f"Scraped page {page} with {len(events_page_data)} events")
            page+=1

        fights_scraping_initializer = controller.select("events",["id","fights","weights"])
        controller.drop("events",["fights","weights"])

    if not args.no_fights:
        if args.no_events:
            page = 1
            fights_scraping_initializer = []
            while True:
                event_ids = attempt_func(event_listing_scraping,{"scraper":scraper,"page":page},args.ignore)
                event_data = attempt_func(event_scraping,{"scraper":scraper,"ids":event_ids,"early_stopping": lambda x: False},args.ignore)
                fights_scraping_initializer.extend(event_data)
                if len(event_ids) == 0:
                    logger.info(f"No more fights found after page {page}.")
                    break
                page+=1
        fights_page_data = attempt_func(fight_scraping,{"scraper":scraper,"events":fights_scraping_initializer,"early_stopping":controller.get_early_stopping("fights")},args.ignore)
        
        controller.insert("fights",fights_page_data,args.prepend)
        
        logger.info(f"Scraped {len(fights_page_data)} fights")

    if not args.no_fighters:
        char=97
        while True:
            page=1
            while True:
                fighter_ids = attempt_func(fighter_listing_scraping, {"scraper":scraper,"char":chr(char),"page":page},args.ignore)
                fighters_page_data = attempt_func(fighter_scraping,{"scraper":scraper,"ids":fighter_ids,"early_stopping":controller.get_early_stopping("fighters")},args.ignore)
                
                if len(fighters_page_data) == 0:
                    logger.info(f"No more fights found.")
                    break
                
                controller.insert("fighters",fighters_page_data,args.prepend)
                page+=1
                break
            if char == 122:
                break
            char+=1
            break
        
        fighters_fights = controller.select("fighters","fights")
        controller.drop("fighters","fights")
        fighters_fights_data = []
        for fighter_fights in fighters_fights:
            fighters_fights_data.extend(fighter_fights["fights"])
        controller.insert("fighter_fights",fighters_fights_data,args.prepend)

    # --- Final save to proper CSVs ---

    if not args.no_fights:
        controller.save("fights",True)
    if not args.no_events:
        controller.save("events",direct=True)
    if not args.no_fighters:
        controller.save("fighters",True)
    if not args.no_fighters and not args.no_fights:
        controller.save("fighter_fights",True)

    return 0


if __name__ == "__main__":
    exit_code = main()
    raise SystemExit(exit_code)
