import strict_rfc3339
import unicodedata
import jsonschema
import random
import hashlib
import string
import re
import datetime
import json
import io
import os

import logging
from . import Config
from . import datestring

try:
    from bookiesports.normalize import IncidentsNormalizer, NotNormalizableException
    from bos_incidents.validator import IncidentValidator
    from bos_incidents.format import incident_to_string
except Exception:
    raise Exception("Please ensure all BOS modules are up to date")

#  %(name) -30s %(funcName) -15s %(lineno) -5d
LOG_FORMAT = ('%(levelname) -10s %(asctime)s: %(message)s')

LOG_FOLDER = os.path.join("dump", "z_logs")


def log_message(message):
    return Config.get("logs", "prefix", default="") + message


def get_log_file_path(log_file_name=None):
    if log_file_name:
        return os.path.join(LOG_FOLDER, log_file_name)
    else:
        return LOG_FOLDER


def save_string(source):
    """ Loads str and bytes object properly into json """
    safe_return = {str: lambda: source,
                   bytes: lambda: source.decode("utf-8")}
    return safe_return[type(source)]()


def search_in(search_for, in_list):
    return search_for.lower() in [x.lower() for x in in_list]


def save_json_loads(source):
    return json.loads(save_string(source))


def slugify(value, allow_unicode=False):
    """ Converts to a file name suitable string

    Convert to ASCII if 'allow_unicode' is False. Convert spaces to hyphens.
    Remove characters that aren't alphanumerics, underscores, or hyphens.
    Convert to lowercase. Also strip leading and trailing whitespace.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode(
            'ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    return re.sub(r'[\s]+', '-', value)


class CommonFormat(object):

    JSON_SCHEMA_CACHED = None
    MASK = None

    @staticmethod
    def get_mask():
        if CommonFormat.MASK is None:
            mask = Config.get("subscriptions", "mask_providers", default=True)
            if type(mask) == bool:
                mask = json.dumps(Config.get("subscriptions", "witnesses")) + json.dumps(Config.get("providers"))
            CommonFormat.MASK = mask
        return CommonFormat.MASK

    @staticmethod
    def get_masked_provider(provider_info):
        masked_name = provider_info["name"] + CommonFormat.get_mask()
        masked_name = hashlib.md5(masked_name.encode()).hexdigest()
        # masking removes everything besides name and pushed
        return {
            "name": masked_name,
            "pushed": provider_info["pushed"]
        }

    def reformat_datetimes(self, formatted_dict):
        """ checks every value, if date found replace with rfc3339 string """
        for (key, value) in formatted_dict.items():
            if value:
                if isinstance(value, dict):
                    self.reformat_datetimes(value)
                elif type(value) == str and len(value) == 19 and\
                        re.match('\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d', str(value)):
                    formatted_dict[key] = date_to_string(value)

    def validate(self, formatted_dict):
        IncidentValidator().validate_incident(formatted_dict)

    def get_id_as_string(self, incident_id):
        return incident_id["start_time"] \
            + '-' + incident_id["sport"] \
            + '-' + incident_id["event_group_name"] \
            + '-' + incident_id["home"] \
            + '-' + incident_id["away"]

    def prepare_for_dump(self, formatted_dict):
        """ reformats dates, validates the json and creates unique_string identifier """
        self.reformat_datetimes(formatted_dict)
        self.validate(formatted_dict)
        # get all to ensure they exist!
        formatted_dict["id"]
        formatted_dict["call"]
        formatted_dict["arguments"]

        # normalize names right away
        formatted_dict = self.normalize_for_witness(formatted_dict)
        formatted_dict["unique_string"] = incident_to_string(formatted_dict)
        return formatted_dict

    def normalize_for_witness(self, validated_incident):
        return IncidentsNormalizer().normalize(validated_incident)


def date_to_string(date_object=None):
    return datestring.date_to_string(date_object)


def string_to_date(date_string=None):
    return datestring.string_to_date(date_string)
