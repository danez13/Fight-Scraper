import pytest
from unittest.mock import MagicMock, patch, call
from scrapers.ufc_stats_scraper import UFCStatsScraper
from exceptions import EntityExistsError

@pytest.fixture
def scraper():
    # Create a fresh scraper instance for each test
    return UFCStatsScraper(wait_time=1, ignore_errors=False, direct=False, update=False)

@pytest.fixture
def scraper_ignore_errors():
    return UFCStatsScraper(wait_time=1, ignore_errors=True, direct=False, update=False)

def test_scrape_event_listings_returns_ids(scraper):
    # Patch fetch_soup and parse_elements to simulate getting event links
    with patch.object(scraper, "fetch_soup") as fetch_soup_mock, \
         patch.object(scraper, "parse_elements") as parse_elements_mock, \
         patch.object(scraper, "parse_Tag_attribute") as parse_attr_mock, \
         patch.object(scraper, "parse_id_from_url") as parse_id_mock:

        # Setup mocks
        fetch_soup_mock.return_value = "fake_soup"
        parse_elements_mock.return_value = ["link1", "link2"]
        parse_attr_mock.side_effect = ["http://example.com/event/1", "http://example.com/event/2"]
        parse_id_mock.side_effect = ["1", "2"]

        ids = scraper.scrape_event_listings(page=1)

        fetch_soup_mock.assert_called_once()
        parse_elements_mock.assert_called_once()
        parse_attr_mock.assert_has_calls([call("link1", "href"), call("link2", "href")])
        parse_id_mock.assert_has_calls([call("http://example.com/event/1"), call("http://example.com/event/2")])
        assert ids == ["1", "2"]

def test_parse_fight_listing_returns_ids(scraper):
    with patch.object(scraper, "parse_elements") as parse_elements_mock, \
         patch.object(scraper, "parse_Tag_attribute") as parse_attr_mock, \
         patch.object(scraper, "parse_id_from_url") as parse_id_mock:

        fake_soup = "fake_soup"
        parse_elements_mock.return_value = ["row1", "row2"]
        parse_attr_mock.side_effect = ["http://example.com/fight/11", "http://example.com/fight/22"]
        parse_id_mock.side_effect = ["11", "22"]

        fight_ids = scraper.parse_fight_listing(fake_soup)

        parse_elements_mock.assert_called_once_with(fake_soup, "tbody.b-fight-details__table-body tr.js-fight-details-click")
        parse_attr_mock.assert_has_calls([call("row1", "data-link"), call("row2", "data-link")])
        parse_id_mock.assert_has_calls([call("http://example.com/fight/11"), call("http://example.com/fight/22")])
        assert fight_ids == ["11", "22"]

def test_scrape_event_raises_entity_exists_error_when_id_exists(scraper):
    scraper.events_dataset = MagicMock()
    scraper.events_dataset.does_id_exist.return_value = True
    scraper.update = False

    with pytest.raises(EntityExistsError):
        scraper.scrape_event("event123")

def test_scrape_event_adds_row_when_id_not_exists(scraper):
    scraper.events_dataset = MagicMock()
    scraper.events_dataset.does_id_exist.return_value = False
    scraper.update = False
    # Patch scraping helpers
    with patch.object(scraper, "fetch_soup") as fetch_soup_mock, \
         patch.object(scraper, "parse_element") as parse_element_mock, \
         patch.object(scraper, "parse_text") as parse_text_mock, \
         patch.object(scraper, "clean_text") as clean_text_mock, \
         patch.object(scraper, "parse_fight_listing") as parse_fight_listing_mock:

        fetch_soup_mock.return_value = "soup"
        parse_element_mock.side_effect = ["title_elem", "date_elem", "location_elem"]
        parse_text_mock.side_effect = ["UFC Event", "Date: Jan 1", "Location: Vegas"]
        clean_text_mock.side_effect = ["UFC Event", "Jan 1", "Vegas"]
        parse_fight_listing_mock.return_value = ["fight1", "fight2"]

        scraper.scrape_event("event123")

        scraper.events_dataset.add_row.assert_called_once_with({
            "id": "event123",
            "title": "UFC Event",
            "date": "Jan 1",
            "location": "Vegas",
            "fights": ["fight1", "fight2"]
        })

def test_scrape_event_updates_row_when_update_true(scraper):
    scraper.events_dataset = MagicMock()
    scraper.events_dataset.does_id_exist.return_value = True
    scraper.update = True

    with patch.object(scraper, "fetch_soup") as fetch_soup_mock, \
         patch.object(scraper, "parse_element") as parse_element_mock, \
         patch.object(scraper, "parse_text") as parse_text_mock, \
         patch.object(scraper, "clean_text") as clean_text_mock, \
         patch.object(scraper, "parse_fight_listing") as parse_fight_listing_mock:

        fetch_soup_mock.return_value = "soup"
        parse_element_mock.side_effect = ["title_elem", "date_elem", "location_elem"]
        parse_text_mock.side_effect = ["UFC Event", "Date: Jan 1", "Location: Vegas"]
        clean_text_mock.side_effect = ["UFC Event", "Jan 1", "Vegas"]
        parse_fight_listing_mock.return_value = ["fight1", "fight2"]

        scraper.scrape_event("event123")

        scraper.events_dataset.update_row.assert_called_once_with("event123", {
            "id": "event123",
            "title": "UFC Event",
            "date": "Jan 1",
            "location": "Vegas",
            "fights": ["fight1", "fight2"]
        })

def test_scrape_fight_raises_entity_exists_error_when_id_exists(scraper):
    scraper.fights_dataset = MagicMock()
    scraper.fights_dataset.does_id_exist.return_value = True
    scraper.update = False

    with pytest.raises(EntityExistsError):
        scraper.scrape_fight("fight123", "event123")

def side_effect_generator_scrape_fight_adds_row_when_id_not_exists(values, default="Default text"):
    it = iter(values)
    while True:
        try:
            yield next(it)
        except StopIteration:
            yield default

def test_scrape_fight_adds_row_when_id_not_exists(scraper):
    scraper.fights_dataset = MagicMock()
    scraper.fights_dataset.does_id_exist.return_value = False
    scraper.update = False

    with patch.object(scraper, "fetch_soup") as fetch_soup_mock, \
         patch.object(scraper, "parse_element") as parse_element_mock, \
         patch.object(scraper, "parse_text") as parse_text_mock, \
         patch.object(scraper, "clean_text") as clean_text_mock, \
         patch.object(scraper, "parse_elements") as parse_elements_mock:

        # Mock soup.select to return two fake fighters
        fake_soup = MagicMock()
        fake_fighter1 = MagicMock()
        fake_fighter2 = MagicMock()
        fake_soup.select.return_value = [fake_fighter1, fake_fighter2]
        fetch_soup_mock.return_value = fake_soup

        # Setup parse_elements for fight details info_items
        parse_elements_mock.return_value = [MagicMock(), MagicMock(), MagicMock()]

        # Setup parse_element side_effect for all expected calls
        parse_element_mock.side_effect = [
            "weight_elem",  # weight element
            "method_elem",  # method element
            "red_name_elem", "red_nickname_elem", "red_result_elem",
            "blue_name_elem", "blue_nickname_elem", "blue_result_elem",
            "label1_elem", "label2_elem", "label3_elem"
        ]

        # Setup parse_text side_effect with a generator to avoid StopIteration
        parse_text_mock.side_effect = side_effect_generator_scrape_fight_adds_row_when_id_not_exists([
            "Lightweight Bout",  # weight text
            "Method: KO",        # method text
            "Red Fighter", "Nickname Red", "Win",
            "Blue Fighter", "Nickname Blue", "Loss",
            "Round:", "Time:", "Other"
        ])

        # Setup clean_text side_effect similarly to parse_text
        clean_text_mock.side_effect = side_effect_generator_scrape_fight_adds_row_when_id_not_exists([
            "Lightweight",  # cleaned weight
            "KO",           # cleaned method
            "Red Fighter", "Nickname Red", "Win",
            "Blue Fighter", "Nickname Blue", "Loss",
            "Round", "Time", "Other"
        ])

        # Run the method under test
        scraper.scrape_fight("fight123", "event123")

        # Verify add_row was called once on fights_dataset
        scraper.fights_dataset.add_row.assert_called_once()

def side_effect_generator(values, default=None):
    it = iter(values)
    while True:
        try:
            yield next(it)
        except StopIteration:
            yield default

def test_scrape_all_runs_complete_flow(scraper):
    with patch.object(scraper, "scrape_event_listings") as scrape_event_listings_mock, \
         patch.object(scraper, "scrape_event") as scrape_event_mock, \
         patch.object(scraper.events_dataset, "save") as events_save_mock, \
         patch.object(scraper, "scrape_fight") as scrape_fight_mock, \
         patch.object(scraper.fights_dataset, "save") as fights_save_mock, \
         patch.object(scraper.events_dataset, "get_instance_column") as get_instance_column_mock:

        scrape_event_listings_mock.return_value = ["event1", "event2", "6420efac0578988b"]

        # Provide at least 3 results for 3 event_ids, default empty list for any further calls
        get_instance_column_mock.side_effect = side_effect_generator([
            ["fight1"], ["fight2"], []
        ], default=[])

        def stop_after_event(event_id):
            if event_id == "6420efac0578988b":
                return False
            return True

        scrape_event_mock.side_effect = stop_after_event
        scrape_fight_mock.return_value = None
        events_save_mock.return_value = None
        fights_save_mock.return_value = None

        result = scraper.scrape_all(page=1)

        assert result is False
        scrape_event_listings_mock.assert_called_once_with(1)
        scrape_event_mock.assert_any_call("event1")
        scrape_event_mock.assert_any_call("event2")
        scrape_event_mock.assert_any_call("6420efac0578988b")

def test_run_executes_passed_function(scraper):
    func = MagicMock(return_value=False)  # Returning False to stop loop immediately
    scraper.run(func)
    func.assert_called_once()

def test_run_handles_entity_exists_error(scraper):
    func = MagicMock(side_effect=EntityExistsError("Entity", "id"))
    # Should break loop, no error raised
    scraper.run(func)
    func.assert_called_once()

def test_run_handles_exceptions_with_ignore_errors(scraper_ignore_errors):
    func = MagicMock(side_effect=[RuntimeError("Test error"), RuntimeError("Test error"), False])
    # Should call func 3 times (2 exceptions caught, then False returned)
    scraper_ignore_errors.run(func)
    assert func.call_count == 3

def test_run_raises_exception_when_ignore_errors_false(scraper):
    func = MagicMock(side_effect=RuntimeError("Test error"))
    with pytest.raises(RuntimeError):
        scraper.run(func)
    func.assert_called_once()

def test_quit_calls_save_methods(scraper):
    with patch.object(scraper.events_dataset, "save") as events_save_mock, \
         patch.object(scraper.fights_dataset, "save") as fights_save_mock:
        scraper.quit(error=True)
        events_save_mock.assert_called_once_with(direct=False)
        fights_save_mock.assert_called_once_with(direct=False)

    with patch.object(scraper.events_dataset, "save") as events_save_mock, \
         patch.object(scraper.fights_dataset, "save") as fights_save_mock:
        scraper.quit(error=False)
        events_save_mock.assert_called_once_with(direct=True)
        fights_save_mock.assert_called_once_with(direct=True)
