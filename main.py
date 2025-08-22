import argparse
import logging
from typing import Callable
from exceptions import EntityExistsError
from scrapers import UFCStatsScraper
from datasets import Dataset
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
        for fight in fights:
            fight["event_id"] = event["id"]
        data_collection.extend(fights)
    return data_collection

def event_scraping(scraper:UFCStatsScraper,ids:list[str],early_stopping:Callable):
    return scraper.run(
                scraper.scrape_events,
                parameters={"ids": ids, "early_stopping": early_stopping},
            )

def event_listing_scraping(scraper:UFCStatsScraper,page:int):
    return scraper.run(scraper.scrape_event_listing,{"page":page})


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
    parser.add_argument("-e", "--events-only", 
                        action="store_true", 
                        help="Scrape only events, not fights")
    parser.add_argument("-f", "--fights-only", 
                        action="store_true", 
                        help="Scrape only fights, not events")
    parser.add_argument("-p","--prepend",
                        action="store_true",
                        help="Save intermediate results to temporary files during scraping")
    args = parser.parse_args(cli_args)

    scraper = UFCStatsScraper(wait_time=3, ignore_errors=args.ignore)

    # Initialize datasets
    events_dataset = Dataset("events", args.update)
    fights_dataset = Dataset("fights", args.update)

    page = 1
    while True:
        # --- Scrape event listing page ---
        event_ids = attempt_func(event_listing_scraping,{"scraper":scraper,"page":page},args.ignore)

        events_page_data = []
        if not args.fights_only:
            # --- Scrape events for this page ---
            events_page_data = attempt_func(event_scraping,{"scraper":scraper,"ids":event_ids,"early_stopping":events_dataset.does_id_exist},args.ignore)
            events_dataset.add_rows(events_page_data,prepend=args.prepend)
            if len(events_page_data) == 0:
                logger.info(f"No more events found after page {page}.")
                break
            events_dataset.save(direct=args.direct)
            logger.info(f"Scraped page {page} with {len(events_page_data)} events")

        # --- Scrape fights for this page ---
        if not args.events_only:
            if args.fights_only:
                events_page_data = attempt_func(event_scraping,{"scraper":scraper,"ids":event_ids,"early_stopping": lambda x: False},args.ignore)
                # For fights-only mode, we need to get fights per event directly
            fights_page_data = attempt_func(fight_scraping,{"scraper":scraper,"events":events_page_data,"early_stopping":fights_dataset.does_id_exist},args.ignore)
            if len(fights_page_data) == 0:
                logger.info(f"No more fights found.")
                break
            fights_dataset.add_rows(fights_page_data,prepend=args.prepend)
            fights_dataset.save(direct=args.direct)
            logger.info(f"Scraped {len(fights_page_data)} fights from page {page}")

        page += 1

    # --- Final save to proper CSVs ---
    if not args.fights_only:
        events_dataset.save(direct=True)
    if not args.events_only:
        fights_dataset.save(direct=True)

    return 0


if __name__ == "__main__":
    exit_code = main()
    raise SystemExit(exit_code)
