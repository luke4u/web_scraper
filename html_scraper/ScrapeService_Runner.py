from html_scraper.ScrapeService import ScrapeService
from html_scraper.config.EngineConfig import EngineConfig
from html_scraper.lib import io_lib
from html_scraper.lib.Logger import StandardLogger


def run_instance(instance_id):
    app_location = io_lib.app_location_path('html_scraper')
    log_file = io_lib.get_log_file(app_location, ending=f'scrape_service_instance_{instance_id}_logger.txt')
    instance_logger = StandardLogger('app', output_filename=log_file)

    try:
        instance_logger.log_info('ScrapeService_Runner', 'Starting ScrapeService instance')

        config_file = io_lib.get_config_file(app_location, ending='yaml/engine_config.yml')
        engine_config = EngineConfig(config_file)

        scrape_service = ScrapeService(engine_config, instance_logger)
        scrape_service.run()

        instance_logger.log_info('ScrapeService_Runner', 'Stopping ScrapeService instance ...')

    except Exception as e:
        instance_logger.log_exception('Main', 'Error ... shutting down ' + str(e))
        raise e


if __name__ == '__main__':
    _instance_id = 1
    run_instance(_instance_id)
