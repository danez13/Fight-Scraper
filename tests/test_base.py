import pytest
from unittest.mock import MagicMock, patch
from requests.exceptions import HTTPError, Timeout
from bs4 import BeautifulSoup, Tag
from scrapers.base import BaseScraper
from exceptions import EntityExistsError


@pytest.fixture
def scraper():
    return BaseScraper(
        base_url="http://example.com/",
        wait_time=1,
        ignore_errors=False,
        direct=False,
        update=False
    )


@pytest.fixture
def soup():
    html = """
    <html>
      <body>
        <div class="test-class" id="test-id" data-attr="value">   Some text with  spaces   </div>
        <p>No class or id</p>
      </body>
    </html>
    """
    return BeautifulSoup(html, "html.parser")


def test_init_sets_session_and_retries(scraper):
    assert scraper.session is not None
    assert hasattr(scraper.session.adapters['http://'], 'max_retries')


@patch('scrapers.base.requests.Session.get')
def test_fetch_soup_success(mock_get, scraper):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.text = "<html></html>"
    mock_get.return_value = mock_response

    soup = scraper.fetch_soup("http://example.com")
    assert isinstance(soup, BeautifulSoup)
    mock_get.assert_called_once()


@patch('scrapers.base.requests.Session.get')
def test_fetch_soup_raises_on_http_error(mock_get, scraper):
    mock_get.side_effect = HTTPError("404 Not Found")
    with pytest.raises(HTTPError):
        scraper.fetch_soup("http://example.com")


def test_parse_elements_returns_elements(soup, scraper):
    elements = scraper.parse_elements(soup, "div.test-class")
    assert len(elements) == 1
    assert elements[0].get('id') == 'test-id'


def test_parse_elements_raises_when_none_found(soup, scraper):
    with pytest.raises(ValueError):
        scraper.parse_elements(soup, ".nonexistent-class")


def test_parse_element_returns_element(soup, scraper):
    element = scraper.parse_element(soup, "div.test-class")
    assert element['id'] == 'test-id'


def test_parse_element_raises_when_none_found(soup, scraper):
    with pytest.raises(ValueError):
        scraper.parse_element(soup, ".nope")


def test_parse_Tag_attribute_returns_attribute(soup, scraper):
    element = soup.select_one("div.test-class")
    value = scraper.parse_Tag_attribute(element, "data-attr")
    assert value == "value"


def test_parse_Tag_attribute_raises_when_attr_missing(soup, scraper):
    element = soup.select_one("div.test-class")
    with pytest.raises(ValueError):
        scraper.parse_Tag_attribute(element, "missing-attr")


def test_parse_text_returns_text(soup, scraper):
    element = soup.select_one("div.test-class")
    text = scraper.parse_text(element)
    assert "Some text" in text


def test_parse_text_raises_on_empty_text():
    scraper = BaseScraper("http://example.com", 1, False, False, False)
    empty_tag = Tag(name="div")
    with pytest.raises(ValueError):
        scraper.parse_text(empty_tag)


@pytest.mark.parametrize("input_text,expected", [
    ("   text  with  spaces  ", "text with spaces"),
    ('"quoted text"', "quoted text"),
    ("'single quoted'", "single quoted"),
    ("'''triple quoted'''", "triple quoted"),
    (None, ""),
    ("", ""),
])
def test_clean_text_variations(scraper, input_text, expected):
    assert scraper.clean_text(input_text) == expected


@pytest.mark.parametrize("url,expected", [
    ("http://site.com/path/12345", "12345"),
    ("http://site.com/path/12345/", "12345"),
    ("http://site.com/", "site.com"),
])
def test_parse_id_from_url(scraper, url, expected):
    if expected == "site.com":
        with pytest.raises(ValueError):
            scraper.parse_id_from_url(url)
    else:
        assert scraper.parse_id_from_url(url) == expected


def test_parse_id_from_url_raises_on_empty_url(scraper):
    with pytest.raises(ValueError):
        scraper.parse_id_from_url("")


def test_run_calls_func_until_false(scraper):
    func = MagicMock(side_effect=[True, True, False])
    scraper.run(func)
    assert func.call_count == 3


def test_run_breaks_on_entity_exists_error(scraper):
    func = MagicMock(side_effect=EntityExistsError("Entity", "id"))
    scraper.run(func)
    func.assert_called_once()


def test_run_handles_exception_ignore_true():
    scraper = BaseScraper(
        base_url="http://example.com/",
        wait_time=1,
        ignore_errors=True,
        direct=False,
        update=False
    )
    func = MagicMock(side_effect=[Exception("fail"), False])
    scraper.run(func)
    assert func.call_count == 2


def test_run_raises_exception_ignore_false():
    scraper = BaseScraper(
        base_url="http://example.com/",
        wait_time=1,
        ignore_errors=False,
        direct=False,
        update=False
    )
    func = MagicMock(side_effect=Exception("fail"))
    with pytest.raises(Exception):
        scraper.run(func)
    func.assert_called_once()
