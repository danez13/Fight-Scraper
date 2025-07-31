import logging

def setup_logging(log_file="scraper.log", level=logging.INFO):
    """Sets up logging for the entire project."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )