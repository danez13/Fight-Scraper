import pytest
from unittest.mock import patch, MagicMock
import logging
import main


@pytest.fixture
def mock_scraper():
    """Patch UFCStatsScraper and yield a mock instance."""
    with patch("main.UFCStatsScraper", autospec=True) as MockScraper:
        instance = MockScraper.return_value
        instance.run = MagicMock()
        instance.quit = MagicMock()
        yield instance


@pytest.fixture
def mock_logging():
    """Patch setup_logging so tests don’t set global logging."""
    with patch("main.setup_logging") as mock_setup:
        yield mock_setup


@pytest.mark.parametrize(
    "scrape_arg, expected_method",
    [
        ("events", "scrape_events"),
        ("fights", "scrape_fights"),
        ("all", "scrape_all"),
    ],
)
def test_main_invokes_correct_scraper_method(mock_scraper, mock_logging, scrape_arg, expected_method):
    """Ensure CLI calls the correct scraper method."""
    exit_code = main.main(["--scrape", scrape_arg])
    assert exit_code == 0
    mock_scraper.run.assert_called_once_with(getattr(mock_scraper, expected_method))
    mock_scraper.quit.assert_called_once_with(error=False)


def test_main_returns_1_on_error(mock_scraper, mock_logging):
    """Test that exceptions cause exit code 1 and quit with error=True."""
    mock_scraper.run.side_effect = RuntimeError("boom")

    exit_code = main.main(["--scrape", "all"])
    assert exit_code == 1
    mock_scraper.quit.assert_called_once_with(error=True)


def test_main_ignore_flag_logs_warning(mock_scraper, mock_logging, caplog):
    """Test that --ignore still logs a warning on errors."""
    mock_scraper.run.side_effect = RuntimeError("boom")

    with caplog.at_level(logging.WARNING):
        exit_code = main.main(["--scrape", "all", "--ignore"])

    assert exit_code == 1
    assert "Ignoring errors" in caplog.text
    mock_scraper.quit.assert_called_once_with(error=True)


def test_setup_logging_called_when_log_true(mock_scraper):
    """setup_logging should be called when log=True."""
    with patch("main.setup_logging") as mock_setup_logging:
        main.main(["--scrape", "all"], log=True)
        mock_setup_logging.assert_called_once_with(level=logging.DEBUG)


def test_setup_logging_not_called_when_log_false(mock_scraper):
    """setup_logging should not be called when log=False."""
    with patch("main.setup_logging") as mock_setup_logging:
        main.main(["--scrape", "all"], log=False)
        mock_setup_logging.assert_not_called()
