import pytest

from html_scraper.ScrapeEngine import ScrapeEngine
from html_scraper.config.EngineConfig import EngineConfig
from html_scraper.lib import io_lib
from html_scraper.lib.Logger import StandardLogger


@pytest.fixture(scope='class')
def location():
    return io_lib.test_location_path('app', 'ci_tests/test_search_engine')


@pytest.fixture(scope='class')
def logger(location):
    log_name = io_lib.get_log_file(location, ending='test_search_engine.txt')
    return StandardLogger('app', output_filename=log_name)


@pytest.fixture(scope='class')
def engine_config():
    app_location = io_lib.app_location_path('app')
    config_file = io_lib.get_config_file(app_location, ending='yaml/engine_config.yml')
    return EngineConfig(config_file)


@pytest.fixture(scope='class')
def requests_engine(logger, engine_config):
    return ScrapeEngine(logger, engine_config)


# TODO: find static url page for testing - LS
class TestScraperEngine(object):

    # TODO: used a random substring for testing for now - LS
    def test__requests_engine_have_1_link_require_1_html(self, requests_engine):
        expected_result = """om" crossorigin="anonymous">
<link rel="preload" href="/webfiles/1629193501226/js/common/set-body-cl"""
        url = 'https://www.metoffice.gov.uk'

        actual_result = requests_engine.run_engine_single(url)[100:200]
        assert actual_result == expected_result

    def test__requests_engine_have_3_link_require_3_html(self, requests_engine):
        expected_result = ["""her-home">
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1, user-scala""",
                           """her-forecast"
      data-location-id="2651048" data-location-name="Dover">
<head>
    <meta name="vi""",
                           """om" crossorigin="anonymous">
<link rel="preload" href="/webfiles/1629193501226/js/common/set-body-cl"""]
        url = ['https://www.bbc.com/weather', 'https://www.bbc.com',
               'https://www.metoffice.gov.uk']

        actual_result = [requests_engine.run_engine_single(_url)[100:200] for _url in url]

        assert actual_result == expected_result
