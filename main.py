import argparse
import logging
from scrapers import UFCStatsScraper
from logging_config import setup_logging

# Setup logging (logs to file + console)
setup_logging()
logger = logging.getLogger(__name__)

# Entry point for script execution
if __name__ == "__main__":
    logger.info("Starting FightIQ Scraper CLI")

    parser = argparse.ArgumentParser(
        prog="FightIQ Scraper",
        description="Scrape UFC page for Event details, and fight details"
    )

    # Scraper arguments
    parser.add_argument("-H", "--headless", action="store_true", help="scrape in headless mode (do not display browser window)")
    parser.add_argument("-c", "--continuous", action="store_true", help="scrape even if previously scraped entries are present")
    parser.add_argument("-w", "--wait", help="set maximum page load wait time", default=10)
    parser.add_argument("-d", "--direct", action="store_true", help="save scraped data directly to final csv files rather than to a separate csv file")
    parser.add_argument("-l", "--pre_linked", action="store_true", help="scrape data based off of links already scraped from the result csv file")
    parser.add_argument("-u", "--update", action="store_true", help="update previously scraped data")

    args = parser.parse_args()
    logger.debug("Parsed CLI arguments: %s", args)

    scraper = UFCStatsScraper(
        headless=args.headless,
        wait_time=args.wait,
        continuous=args.continuous,
        direct=args.direct,
        pre_linked=args.pre_linked,
        update=args.update
    )

    error = False
    try:
        logger.info("Running scraper...")
        scraper.run()
        logger.info("Scraper finished successfully.")
    except Exception as e:
        logger.exception("An error occurred during scraping.")
        error = True
    finally:
        logger.info("Cleaning up and quitting scraper (error=%s)...", error)
        scraper.quit(error=error)
