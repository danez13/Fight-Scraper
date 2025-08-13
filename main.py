import argparse
import logging
from scrapers import UFCStatsScraper
from datasets import Dataset
from logging_config import setup_logging

logger = logging.getLogger(__name__)

def main(cli_args=None, log=True) -> int:

    if log:
        setup_logging(level=logging.DEBUG)

    logger.info("Starting FightIQ Scraper CLI")

    parser = argparse.ArgumentParser(
        prog="FightIQ Scraper",
        description="Scrape UFC page for Event details, and fight details"
    )

    # Scraper arguments
    parser.add_argument(
        "-i", "--ignore", action="store_true",
        help="ignore any errors that occur during scraping"
    )
    parser.add_argument(
        "-w", "--wait", default=10, type=int,
        help="Set maximum page load wait time"
    )
    parser.add_argument(
        "-d", "--direct", action="store_true",
        help="Save scraped data directly to final CSV files rather than to a temp file"
    )
    parser.add_argument(
        "-u", "--update", action="store_true",
        help="Update previously scraped data"
    )

    args = parser.parse_args(cli_args)
    logger.debug("Parsed CLI arguments: %s", args)

    scraper = UFCStatsScraper(
        wait_time=args.wait,
        ignore_errors=args.ignore,
        direct=args.direct,
        update=args.update
    )

    error = False
    try:
        logger.info("Running scraper...")
        scraper.run()
        logger.info("Scraper finished successfully.")
        return 0
    except Exception as e:
        logger.error("An error occurred during scraping: %s", e)
        if args.ignore:
            logger.warning("Ignoring errors due to --ignore flag.")
        else:
            logger.exception("Exiting due to an unhandled exception.")
        error = True
        return 1
    finally:
        logger.info("Cleaning up and quitting scraper (error=%s)...", error)
        scraper.quit(error=error)

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)