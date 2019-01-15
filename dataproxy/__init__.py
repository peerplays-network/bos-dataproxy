import os
import yaml
import io
from copy import deepcopy
import logging
import collections
from logging.handlers import TimedRotatingFileHandler

from bookiesports.normalize import IncidentsNormalizer


def get_version():
    try:
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'VERSION')) as version_file:
            return version_file.read().strip()
    except FileNotFoundError:
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", 'VERSION')) as version_file:
            return version_file.read().strip()


__VERSION__ = get_version()


class Config(dict):
    """ This class allows us to load the configuration from a YAML encoded
        configuration file.
    """

    ERRORS = {
    }

    data = None
    source = None

    @staticmethod
    def load(config_files=[], relative_location=False):
        """ Load config from a file

            :param str file_name: (defaults to 'config.yaml') File name and
                path to load config from
        """
        if not Config.data:
            Config.data = {}

        if not config_files:
            raise Exception("Trying to load config without target files")
        if type(config_files) == str:
            config_files = [config_files]

        for config_file in config_files:
            if relative_location:
                file_path = config_file
            else:
                file_path = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    config_file
                )
            stream = io.open(file_path, 'r', encoding='utf-8')
            with stream:
                Config.data = Config._nested_update(Config.data, yaml.load(stream))

        Config.source = ";".join(config_files)

    @staticmethod
    def get_config(config_name=None):
        """ Static method that returns the configuration as dictionary.
            Usage:

            .. code-block:: python

                Config.get_config()
        """
        if not config_name:
            if not Config.data:
                raise Exception("Either preload the configuration or specify config_name!")
        else:
            if not Config.data:
                Config.data = {}
            Config.load(config_name)
        return deepcopy(Config.data)

    @staticmethod
    def get(*args, **kwargs):
        """
        This config getter method allows sophisticated and encapsulated access to the config file, while
        being able to define defaults in-code where necessary.

        :param args: key to retrieve from config, nested in order. if the last is not a string it is assumed to be the default, but giving default keyword is then forbidden
        :type tuple of strings, last can be object
        :param message: message to be displayed when not found, defaults to entry in ERRORS dict with the
                                key defined by the desired config keys in args (key1.key2.key2). For example
                                Config.get("foo", "bar") will attempt to retrieve config["foo"]["bar"], and if
                                not found raise an exception with ERRORS["foo.bar"] message
        :type message: string
        :param default: default value if not found in config
        :type default: object
        """
        default_given = "default" in kwargs
        default = kwargs.pop("default", None)
        message = kwargs.pop("message", None)
        # check if last in args is default value
        if type(args[len(args) - 1]) != str:
            if default_given:
                raise KeyError("There can only be one default set. Either use default=value or add non-string values as last positioned argument!")
            default = args[len(args) - 1]
            default_given = True
            args = args[0:len(args) - 1]

        try:
            nested = Config.data
            for key in args:
                if type(key) == str:
                    nested = nested[key]
                else:
                    raise KeyError("The given key " + str(key) + " is not valid.")
            if nested is None:
                raise KeyError()
        except KeyError:
            lookup_key = '.'.join(str(i) for i in args)
            if not message:
                if Config.ERRORS.get(lookup_key):
                    message = Config.ERRORS[lookup_key]
                else:
                    message = "Configuration key {0} not found in {1}!"
                message = message.format(lookup_key, Config.source)
            if default_given:
                logging.getLogger(__name__).debug(message + " Using given default value.")
                return default
            else:
                raise KeyError(message)

        # filter out empty lists
        if type(nested) == list and len(nested) == 1 and nested[0] is None:
            nested = None

        return nested

    @staticmethod
    def reset():
        """ Static method to reset the configuration storage
        """
        Config.data = None
        Config.source = None

    @staticmethod
    def _nested_update(d, u):
        for k, v in u.items():
            if isinstance(v, collections.Mapping):
                d[k] = Config._nested_update(d.get(k, {}), v)
            else:
                if d:
                    d[k] = v
                else:
                    d = {}
                    d[k] = v
        return d


def set_global_logger(existing_loggers=None, config_file_name=None):
    print("Setting up logger handling for dataproxy...")

    # setup logging
    # ... log to file system
    log_folder = os.path.join(Config.get("dump_folder", default="dump"), Config.get("logs", "folder", default="logs"))
    log_level = logging.getLevelName(Config.get("logs", "level", default="INFO"))

    os.makedirs(log_folder, exist_ok=True)
    log_format = (Config.get("logs", "format", default="%(asctime)s %(levelname) -10s %(name)s: %(message)s"))
    if config_file_name is None:
        config_file_name = Config.get("logs", "file", default="dataproxy.log")
    trfh = TimedRotatingFileHandler(
        os.path.join(log_folder, config_file_name),
        "midnight",
        1
    )
    trfh.suffix = "%Y-%m-%d"
    trfh.setFormatter(logging.Formatter(log_format))
    trfh.setLevel(log_level)

    # ... and to console
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter(log_format))
    sh.setLevel(log_level)

    # global config (e.g. for werkzeug)
    logging.basicConfig(level=log_level,
                        format=log_format,
                        handlers=[trfh, sh])

    use_handlers = [trfh, sh]

    if existing_loggers is not None:
        if not type(existing_loggers) == list:
            existing_loggers = [existing_loggers]
        for logger in existing_loggers:
            logger.setLevel(log_level)
            while len(logger.handlers) > 0:
                logger.removeHandler(logger.handlers[0])
            for handler in use_handlers:
                logger.addHandler(handler)

    print("... done")
    return use_handlers


def on_startup():
    if Config.data and Config.data.get("subscribed_witnesses", None) is not None:
        raise Exception("Please update your config.yaml to match the new format, subscribed_witnesses is outdated")

    Config.get("subscriptions", "mask_providers")

    try:
        IncidentsNormalizer.use_chain(Config.get("bookiesports_chain", default="beatrice"),
                                      not_found_file=os.path.join(Config.get("dump_folder"), "missing_bookiesports_entries.txt"))
    except AttributeError:
        IncidentsNormalizer.DEFAULT_CHAIN = Config.get("bookiesports_chain", default="beatrice")
        IncidentsNormalizer.NOT_FOUND_FILE = os.path.join(Config.get("dump_folder"), "missing_bookiesports_entries.txt")
        logging.getLogger(__name__).debug("Incidents normalizer set for chain " + IncidentsNormalizer.DEFAULT_CHAIN + ", using " + str(IncidentsNormalizer.NOT_FOUND_FILE) + " for missing entries")


if not Config.data:
    Config.load("config-defaults.yaml")
    notify = False
    try:
        # overwrites defaults
        Config.load("config-dataproxy.yaml", True)
        notify = True
    except FileNotFoundError:
        pass
    try:
        # overwrites defaults
        Config.load("../config-dataproxy.yaml", True)
        notify = True
    except FileNotFoundError:
        pass

    set_global_logger()

    on_startup()

    if notify:
        # don't use utils here due to import loop
        logging.getLogger(__name__).info("Custom config has been loaded from working directory: " + Config.source)
    else:
        raise Exception("No custom config has been found in working directory (filename should be config-dataproxy.yaml)")
