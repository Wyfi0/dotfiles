import logging
from io import StringIO
from typing import Optional

from .env import PoliigonEnvironment


# Numerical comment -> values for .ini
NOT_SET = logging.NOTSET  # 0
DEBUG = logging.DEBUG  # 10
INFO = logging.INFO  # 20
WARNING = logging.WARNING  # 30
ERROR = logging.ERROR  # 40
CRITICAL = logging.CRITICAL  # 50


def initialize_logger(module_name: Optional[str] = None,
                      *,
                      log_lvl: Optional[int] = None,
                      env: Optional[PoliigonEnvironment] = None,
                      log_stream: Optional[StringIO] = None,
                      base_name: str = "Addon"
                      ) -> logging.Logger:
    """Set format, log level and returns a logger instance

    Args:
       module_name: The name of the module a required argument
                    Env log_lvl variable name is derived as follows:
                    Logger name: Addon => log_lvl
                    Logger name: Addon.DL => log_lvl_dl
                    Logger name: Addon.P4C.UI => log_lvl_p4c_ui
                    But also:
                    Logger name: bonnie => log_lvl
                    Logger name: clyde.whatever => log_lvl_whatever
       log_lvl: Integer specifying which logs to be printed, one of:
            https://docs.python.org/3/library/logging.html#levels
       env: If log_lvl is None, env will be used to set log_lvl
       log_stream: Output to StringIO stream instead of the console if not None
       base_name: By default all loggers get derived from logger "Addon".

    Returns:
        Returns a reference to the initialized logger instance

    Raises:
        AttributeError: If log_lvl and env are both None.
    """

    if module_name is None:
        logger_name = f"{base_name}"
        name_hierarchy = []
    else:
        logger_name = f"{base_name}.{module_name}"
        name_hierarchy = module_name.split(".")

    if log_lvl is None:
        log_lvl_name = "log_lvl"
        for name in name_hierarchy:
            log_lvl_name += f"_{name.lower()}"

        try:
            log_lvl = env.config.getint("DEFAULT", log_lvl_name, fallback=NOT_SET)
        except AttributeError:
            log_lvl = NOT_SET  # no .ini

    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.setLevel(log_lvl)
    format_input = (
        f"%(name)s, %(levelname)s, %(asctime)s, %(filename)s/%(funcName)s:%(lineno)d: %(message)s"
    )
    date_format = "%I:%M:%S"

    formatter = logging.Formatter(fmt=format_input, datefmt=date_format)
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    return logger
