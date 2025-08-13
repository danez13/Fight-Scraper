import pytest
from bs4 import BeautifulSoup, Tag
import requests
from unittest.mock import patch, Mock
from scrapers import base  # Adjust import path if needed


class DummyScraper(base.BaseScraper):
    def run(self):
        pass


@pytest.fixture
def scraper():
    return DummyScraper(
        base_url="http://example.com/",
        wait_time=1,
        ignore_errors=False,
        direct=False,
        update=False
    )


def test_fetch_soup_success(scraper):
    mock_response = Mock()
    mock_response.text = "<html><p>hello</p></html>"
    mock_response.raise_for_status = Mock()

    with patch.object(scraper.session, "get", return_value=mock_response):
        soup = scraper.fetch_soup("http://test.com")

    assert isinstance(soup, BeautifulSoup)
    assert soup.p is not None
    assert soup.p.text == "hello"


def test_fetch_soup_failure(scraper):
    with patch.object(scraper.session, "get", side_effect=requests.RequestException("fail")):
        with pytest.raises(requests.RequestException):
            scraper.fetch_soup("http://bad.com")


def test_parse_elements_found(scraper):
    soup = BeautifulSoup("<div><span class='x'>1</span></div>", "html.parser")
    elements = scraper.parse_elements(soup, ".x")
    assert len(elements) == 1
    assert isinstance(elements[0], Tag)
    assert elements[0].text == "1"


def test_parse_elements_not_found(scraper):
    soup = BeautifulSoup("<div></div>", "html.parser")
    with pytest.raises(ValueError, match="No elements found"):
        scraper.parse_elements(soup, ".missing")


def test_parse_element_found(scraper):
    soup = BeautifulSoup("<div><span id='y'>hi</span></div>", "html.parser")
    elem = scraper.parse_element(soup, "#y")
    assert isinstance(elem, Tag)
    assert elem.text == "hi"


def test_parse_element_not_found(scraper):
    soup = BeautifulSoup("<div></div>", "html.parser")
    with pytest.raises(ValueError, match="No element found"):
        scraper.parse_element(soup, "#nope")


def test_parse_Tag_attribute_found(scraper):
    tag = BeautifulSoup('<a href="http://link.com"></a>', "html.parser").a
    assert scraper.parse_Tag_attribute(tag, "href") == "http://link.com"


def test_parse_Tag_attribute_missing(scraper):
    tag = BeautifulSoup("<a></a>", "html.parser").a
    with pytest.raises(ValueError, match="Attribute 'href' not found"):
        scraper.parse_Tag_attribute(tag, "href")


def test_parse_text_found(scraper):
    tag = BeautifulSoup("<p> hello </p>", "html.parser").p
    assert scraper.parse_text(tag) == " hello "


def test_parse_text_missing(scraper):
    tag = BeautifulSoup("<p></p>", "html.parser").p
    with pytest.raises(ValueError, match="No text found"):
        scraper.parse_text(tag)


import pytest

@pytest.mark.parametrize("input_text,expected", [
    ("  some   text  ", "some text"),
    ("", ""),
    (None, "")
])
def test_clean_text(scraper, input_text, expected):
    assert scraper.clean_text(input_text) == expected


def test_parse_id_from_url_valid(scraper):
    url = "http://example.com/abc123"
    assert scraper.parse_id_from_url(url) == "abc123"


@pytest.mark.parametrize("url", ["", None])
def test_parse_id_from_url_invalid(scraper, url):
    with pytest.raises(ValueError, match="Could not extract ID"):
        scraper.parse_id_from_url(url)


def test_run_abstract_method():
    class DummyScraper(base.BaseScraper):
        def run(self):
            raise NotImplementedError("Subclasses must implement this method")

    dummy = DummyScraper(
        base_url="http://example.com/",
        wait_time=1,
        ignore_errors=False,
        direct=False,
        update=False
    )

    with pytest.raises(NotImplementedError):
        dummy.run()