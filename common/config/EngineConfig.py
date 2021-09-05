import yaml


class EngineConfig(object):
    def __init__(self, config_file):
        engine_config = self.engine_config(config_file)
        self.lang = engine_config['lang']
        self.timeout = engine_config['timeout']
        self.environment = engine_config['environment']

    def is_dev(self):
        return self.environment == 'dev'

    def is_prod(self):
        return self.environment == 'prod'

    def get_mode(self):
        return self.environment

    @classmethod
    def engine_config(cls, config_file):
        with open(config_file) as f:
            engine_config = yaml.load(f, Loader=yaml.FullLoader)['engine']
        return engine_config
