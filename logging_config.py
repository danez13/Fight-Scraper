import logging

class AnsiColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        no_style = '\033[0m'
        bold = '\033[91m'
        yellow = '\033[93m'
        green_light = '\033[92m'
        red = '\033[31m'
        red_light = '\033[91m'
        cyan = '\033[36m'
        
        start_style = {
            'DEBUG': cyan,
            'INFO': no_style,
            'WARNING': yellow,
            'ERROR': red,
            'CRITICAL': red_light + bold,
        }.get(record.levelname, no_style)
        end_style = no_style
        return f'{start_style}{super().format(record)}{end_style}'

def setup_logging(log_file="scraper.log", level=logging.INFO):
    """Sets up logging for the entire project with color-coded console output."""

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()  # Prevent duplicate handlers on reload

    # File handler (no color)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    ))

    # Console handler (with color)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(AnsiColorFormatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)