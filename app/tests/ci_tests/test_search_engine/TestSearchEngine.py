import pandas as pd
import pytest

from app.SearchEngine import SearchEngine
from common.config.EngineConfig import EngineConfig
from common.lib import io_lib
from common.lib.Logger import StandardLogger


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
def search_engine(logger, engine_config):
    return SearchEngine(logger, engine_config)


class TestSearchEngine(object):

    def test__have_1_term_require_1_result(self):
        # arrange
        expected_result = ['https://www.bbc.com/weather']
        num = 1
        term = 'weather'

        # act
        actual_result = SearchEngine.run_single(term, num)

        # assert
        assert actual_result == expected_result

    def test__have_1_term_require_3_results(self):
        # arrange
        expected_result = ['https://www.bbc.com/weather', 'https://www.bbc.com/weather/2651048',
                           'https://www.metoffice.gov.uk/weather/forecast/u10fvgptv']
        num = 3
        term = 'weather'

        # act
        actual_result = SearchEngine.run_single(term, num)

        # assert
        assert actual_result == expected_result

    def test__df_have_no_term_require_no_result(self, search_engine):
        # arrange
        expected_result = pd.DataFrame(columns=['term', 'url']).astype({'term': 'str', 'url': 'str'})
        test_data = pd.DataFrame()

        # act
        actual_result = search_engine.run(test_data)

        # assert
        self.assert_are_equal(actual_result, expected_result)

    def test__df_have_1_term_require_3_result(self, search_engine):
        # arrange
        url = ['https://www.bbc.com/weather', 'https://www.bbc.com/weather/2651048',
               'https://www.metoffice.gov.uk/weather/forecast/u10fvgptv']
        term = ['weather'] * 3
        expected_result = pd.DataFrame({'term': term, 'url': url}).astype({'term': 'str', 'url': 'str'})
        test_data = pd.DataFrame({'term': ['weather'], 'num': 3}).astype({'term': 'str', 'num': 'int64'})

        # act
        actual_result = search_engine.run(test_data)

        # assert
        self.assert_are_equal(actual_result, expected_result)

    def test__df_have_only_invalid_term_require_no_result(self, search_engine):
        # arrange
        expected_result = pd.DataFrame(columns=['term', 'url'])
        test_data = pd.DataFrame({'term': None, 'num': [-3]})

        # act
        actual_result = search_engine.run(test_data).dropna(subset=['term'])

        # assert
        self.assert_are_equal(actual_result, expected_result)

    @staticmethod
    def assert_are_equal(actual_result, expected_result):
        assert len(actual_result) == len(expected_result)
        if not actual_result.equals(expected_result):
            for i in range(actual_result.shape[0]):
                pd.testing.assert_series_equal(actual_result.iloc[i], expected_result.iloc[i], check_exact=False,
                                               atol=1e-5, check_names=False)
