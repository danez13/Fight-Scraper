import pytest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from scrapers.ufc_stats_scraper import UFCStatsScraper

# Mock Dataset to replace the real one in tests
class MockDataset:
    def __init__(self, *args, **kwargs):
        self.data = []
        self.saved = False
        self.saved_direct = None

    def does_id_exist(self, id):
        return any(row.get("id") == id for row in self.data)

    def add_row(self, row):
        if not isinstance(row, dict):
            raise ValueError("Row must be a dictionary.")
        self.data.append(row)

    def update_row(self, id, new_row):
        for i, row in enumerate(self.data):
            if row.get("id") == id:
                self.data[i] = {**row, **new_row}
                return
        raise ValueError(f"ID {id} does not exist in the dataset.")

    def save(self, direct=False):
        self.saved = True
        self.saved_direct = direct


@pytest.fixture
def mock_dataset():
    return MockDataset()


@pytest.fixture
def scraper(mock_dataset, monkeypatch):
    # Patch the Dataset class inside UFCStatsScraper to return mock_dataset
    monkeypatch.setattr("scrapers.ufc_stats_scraper.Dataset", lambda *args, **kwargs: mock_dataset)
    return UFCStatsScraper(wait_time=1, ignore_errors=False, direct=False, update=False)


def test_quit_behavior(scraper, mock_dataset):
    # Test quit with error True
    scraper.quit(error=True)
    assert mock_dataset.saved is True
    assert mock_dataset.saved_direct is False

    # Reset for next call
    mock_dataset.saved = False
    mock_dataset.saved_direct = None

    # Test quit with error False
    scraper.quit(error=False)
    assert mock_dataset.saved is True
    assert mock_dataset.saved_direct is True