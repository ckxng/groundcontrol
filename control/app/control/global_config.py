from control.config import Config

# create global config
global_config = Config()
global_config.update_from_environ()
