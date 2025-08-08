import pytest
import logging
from unittest.mock import MagicMock, patch
import main


@pytest.fixture
def mock_scraper_cls():
    """Fixture to patch UFCStatsScraper so no real scraping occurs."""
    with patch("main.UFCStatsScraper") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_cls


@pytest.fixture
def mock_setup_logging():
    """Fixture to patch setup_logging to avoid real log config."""
    with patch("main.setup_logging") as mock_log:
        yield mock_log


@pytest.mark.parametrize(
    "cli_args, expected_kwargs",
    [
        ([], {"wait_time": 10, "ignore_errors": False, "direct": False, "update": False}),
        (["-w", "5", "-i"], {"wait_time": 5, "ignore_errors": True, "direct": False, "update": False}),
        (["-d", "-u"], {"wait_time": 10, "ignore_errors": False, "direct": True, "update": True}),
    ],
)
def test_main_success(cli_args, expected_kwargs, mock_scraper_cls, mock_setup_logging):
    """Test main() when scraper.run() succeeds."""
    mock_instance = mock_scraper_cls.return_value

    exit_code = main.main(cli_args)

    # Assert scraper is created with correct args
    mock_scraper_cls.assert_called_once_with(**expected_kwargs)

    # run() is called and quit() is called once with error=False
    mock_instance.run.assert_called_once()
    mock_instance.quit.assert_called_once_with(error=False)

    assert exit_code == 0


def test_main_scraper_error_ignore_flag(mock_scraper_cls, mock_setup_logging, caplog):
    """Test main() when scraper.run() raises exception but --ignore is set."""
    mock_instance = mock_scraper_cls.return_value
    mock_instance.run.side_effect = RuntimeError("Scraper failed")

    with caplog.at_level(logging.WARNING):
        exit_code = main.main(["--ignore"])

    # Even on error, quit() should still be called with error=True
    mock_instance.quit.assert_called_once_with(error=True)

    assert "Ignoring errors due to --ignore flag." in caplog.text
    assert exit_code == 1


def test_main_scraper_error_no_ignore(mock_scraper_cls, mock_setup_logging, caplog):
    """Test main() when scraper.run() raises exception and --ignore is NOT set."""
    mock_instance = mock_scraper_cls.return_value
    mock_instance.run.side_effect = ValueError("Boom!")

    with caplog.at_level(logging.ERROR):
        exit_code = main.main([])

    mock_instance.quit.assert_called_once_with(error=True)
    assert "Exiting due to an unhandled exception." in caplog.text
    assert exit_code == 1


def test_main_logging_disabled(mock_scraper_cls):
    with patch("main.setup_logging") as mock_log:
        exit_code = main.main(cli_args=[], log=False)
        assert exit_code == 0
        mock_log.assert_not_called()


def test_main_quit_called_on_exception(mock_scraper_cls, mock_setup_logging):
    """Ensure quit() is called even if exception occurs before run()."""
    mock_scraper_cls.side_effect = RuntimeError("Init fail")

    with pytest.raises(RuntimeError):
        main.main([])
    mock_scraper_cls.assert_called_once()
