import requests
import os
import json
import io
import logging
import random
import hashlib
import time
from time import strptime
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from . import utils
from . import Config
from .utils import CommonFormat


class GenericProcessor(ABC):

    SHUFFLED_SUBSCRIBERS_PER_GROUP = None
    SHUFFLED_SUBSCRIBERS_EXPIRES = None

    @staticmethod
    def get_timed_shuffled_subscribers(targets=None):
        now = datetime.utcnow()

        try:
            subscribers = Config.get("subscriptions", "witnesses")
        except KeyError:
            logging.getLogger(__name__).debug(
                "No witness has subscribed to this data proxy, incidents are only archived now"
            )
            return []

        if GenericProcessor.SHUFFLED_SUBSCRIBERS_PER_GROUP is None or\
                now > GenericProcessor.SHUFFLED_SUBSCRIBERS_EXPIRES:
            subscribers = subscribers.copy()
            random.shuffle(subscribers)

            subscribers_per_group = {}
            # build list according to distinct chains
            for subscriber in subscribers:
                if subscriber is None:
                    continue
                if type(subscriber) == str:
                    subscriber = {"url": subscriber.strip(),
                                  "group": "none"}
                else:
                    subscriber = {"url": subscriber["url"].strip(),
                                  "group": subscriber.get("group", "none"),
                                  "name": subscriber.get("name", None)}
                    if subscriber.get("whitelist_providers", None) is not None:
                        subscriber["whitelist_providers"] = subscriber.get("whitelist_providers", None)
                if subscribers_per_group.get(subscriber["group"], None) is None:
                    subscribers_per_group[subscriber["group"]] = []
                subscribers_per_group[subscriber["group"]].append(subscriber)
            GenericProcessor.SHUFFLED_SUBSCRIBERS_PER_GROUP = subscribers_per_group
            GenericProcessor.SHUFFLED_SUBSCRIBERS_EXPIRES = now + timedelta(
                hours=Config.get("subscriptions", "shuffled_subscribers_expires_after_in_hours", 6)
            )
            logging.getLogger(__name__).debug("Shuffled witnesses: " + str(GenericProcessor.SHUFFLED_SUBSCRIBERS_PER_GROUP))

        if targets is not None:
            return_value = {}
            # filter out non targets
            for group in GenericProcessor.SHUFFLED_SUBSCRIBERS_PER_GROUP.keys():
                for witness in GenericProcessor.SHUFFLED_SUBSCRIBERS_PER_GROUP[group]:
                    if witness["url"] in [x["url"] for x in targets]:
                        if return_value.get(group, None) is None:
                            return_value[group] = []
                        return_value[group].append(witness)
            return return_value
        else:
            return GenericProcessor.SHUFFLED_SUBSCRIBERS_PER_GROUP

    """ Processes interesting parsed files from a provider push and searches for incidents """
    def __init__(self, file_ending=None, file_cache_store=None):
        self._allowed_file_ending = file_ending
        self._file_cache_store = file_cache_store
        self._debug_identifiers = None

        self._skip_on_error = False

    def debug_games(self, identifiers):
        if isinstance(identifiers, str):
            self._debug_identifiers = [identifiers.lower()]
        elif isinstance(identifiers, int):
            self._debug_identifiers = [identifiers]
        else:
            self._debug_identifiers = identifiers

    def process_generic(self,
                        files=None,
                        folder=None,
                        as_string=None,
                        folder_filter=None,
                        name_filter=None):
        if files is None:
            files = []
        if folder is None:
            folder = []
        if as_string is None:
            as_string = []
        # unify all sources into all_sources field
        self.folder = folder
        if isinstance(as_string, str):
            as_string = [as_string]
        if isinstance(files, str):
            files = [files]
        self.files = files
        self.files_as_string = as_string
        if type(folder_filter) == set:
            folder_filter = list(folder_filter)
        self.folder_filter = folder_filter
        if type(name_filter) == str:
            name_filter = [name_filter]
        self.name_filter = name_filter
        self._populate_source_list()
        # find incidents in given sources
        return self._find_incidents()

    def process(self, as_string):
        return self.process_generic(as_string=as_string)

    def skip_on_error(self):
        self._skip_on_error = True

    def _is_allowed_file(self, file):
        return self._allowed_file_ending and file.endswith(self._allowed_file_ending)

    def _is_filtered_subfolder(self, folder, abs_path):
        # no filter set, descend into all
        if not self.folder_filter:
            logging.getLogger(__name__).debug("No filter, adding " + abs_path)
            return True

        # folder is directly containted, take it
        if folder in self.folder_filter:
            logging.getLogger(__name__).debug("Direct match " + abs_path)
            return True

        # special case: last filder can have special meaning
        if self.folder_filter[len(self.folder_filter) - 1].startswith("after:") and folder.startswith("2"):
            after_folder = self.folder_filter[len(self.folder_filter) - 1].split("after:")[1]
            after_folder = strptime(after_folder, "%Y%m%d")
            folder_time = strptime(folder, "%Y%m%d")
            if (after_folder < folder_time):
                logging.getLogger(__name__).debug("Special action after: true " + abs_path)
                return True

        # matched with startswith
        if folder.startswith(tuple(self.folder_filter)):
            logging.getLogger(__name__).debug("Matched with startswith! " + abs_path)
            return True

        # special case: only provider given, always dive into date folders (starting with 2[018])
        if len(self.folder_filter) == 1 and not self.folder_filter[0].startswith("2") and folder.startswith("2"):
            logging.getLogger(__name__).debug("Only provider given, using all date folders! " + abs_path)
            return True

    def _populate_folder(self, folder):
        for file in os.listdir(folder):
            abs_path = os.path.join(folder, file)
            if os.path.isdir(abs_path):
                if self._is_filtered_subfolder(file, abs_path):
                    self._populate_folder(abs_path)
            else:
                if self._is_allowed_file(file):
                    self.files.append(abs_path)

    def _populate_source_list(self):
        if not self.files and self.folder:
            self._populate_folder(self.folder)
        self.all_sources = []
        for item in self.files:
            self._add_as_source(item)
        for item in self.files_as_string:
            self._add_as_source(item)

    def _add_as_source(self, file_name):
        if self.name_filter is not None:
            match_to = os.path.basename(file_name.lower())
            add = True
            for tmp in self.name_filter:
                if tmp.lower() not in match_to:
                    add = False
            if add:
                self.all_sources.append(file_name)
        else:
            self.all_sources.append(file_name)

    def _parse_raw(self, raw_file_content):
        raise Exception("Abstract method")

    def _find_incidents(self):
        incidents = {}
        for source in self.all_sources:
            if source.endswith(".raw"):
                source = self._parse_raw(io.open(source, encoding="utf-8").read())
                if not source or not self.source_of_interest(source):
                    continue
            incident = None
            try:
                # file or string?
                # TODO rework how files are recognized
                if self._is_allowed_file(source):
                    incidentList = self._process_source(source, "file")
                else:
                    incidentList = self._process_source(source, "string")
                # one or many found?
                if incidentList:
                    if isinstance(incidentList, dict):
                        incidentList = [incidentList]
                    for incident in incidentList:
                        # ignore unkown
                        if incident["call"] == "unknown":
                            continue
                        # ensure the json format is correct
                        incident = CommonFormat().prepare_for_dump(incident)
                        # use unique_string for duplicate prevention
                        unique_string = incident["unique_string"] + incident["provider_info"]["name"]
                        # only return it if its interesting
                        if self._incident_of_interest(incident) and\
                                not incidents.get(unique_string):
                            incident["timestamp"] = utils.date_to_string()
                            incidents[unique_string] = incident
                            logging.getLogger(__name__).debug("incident found: " + incident["unique_string"])
            except Exception as e:
                message = None
                if incident:
                    message = str(incident)
                else:
                    message = str(source)
                logging.getLogger(__name__).error("Error parsing, " + str(e) + "\n" + message)
                if not self._skip_on_error:
                    raise e
        return incidents.values()

    def _resolve_json_with_file_cache(self,
                                      identifier,
                                      getter=None,
                                      overwrite=False):
        """ If a file with the identifier exists, return its content.
            Otherwise call getter, store content in file and return """
        if not getter:
            def getter():
                return None
        if self._file_cache_store:
            sub_folder = self.__class__.__name__
            identifier = utils.slugify(identifier)
            if not overwrite and self._file_cache_store.exists(sub_folder,
                                                               ".json",
                                                               identifier):
                stream, stream_len = self._file_cache_store.open(
                    sub_folder,
                    identifier + ".json"
                )
                return utils.save_json_loads(stream.read(stream_len))
            else:
                json_object = getter()
                if json_object:
                    self._file_cache_store.save(sub_folder,
                                                json.dumps(json_object),
                                                file_ext=".json",
                                                file_name=identifier)
                return json_object
        else:
            return getter()

    @abstractmethod
    def _process_source(self, source, source_type):
        return None

    def source_of_interest(self, source):
        return True

    @abstractmethod
    def _incident_of_interest(self, incident):
        return True

    def _do_post(self, url, json_content):
        retries = Config.get("subscriptions", "retry_on_error", "number", 1)
        delay = Config.get("subscriptions", "retry_on_error", "delay", 2)
        while True:
            try:
                return requests.post(url, json=json_content, timeout=1)
            except Exception as e:
                if retries > 0:
                    retries = retries - 1
                    time.sleep(delay)
                else:
                    raise e

    def send_to_witness(self, incident, targets=None):
        unmasked_provider_info = incident["provider_info"]

        mask = Config.get("subscriptions", "mask_providers", default=True)
        if mask:
            incident["provider_info"] = CommonFormat.get_masked_provider(incident["provider_info"])

        try:
            whitelist_providers = Config.get("subscriptions", "whitelist_providers")
        except KeyError:
            whitelist_providers = None

        if whitelist_providers is not None and unmasked_provider_info["name"] not in whitelist_providers:
            logging.getLogger(__name__).debug(
                "Not sending incident, provider not found in config list subscribed_witnesses_send"
            )
            return {}

        subscribed_witnesses_status = {}

        shuffled_per_group = GenericProcessor.get_timed_shuffled_subscribers(targets)
        delay_to_next = Config.get("subscriptions", "delay_to_next_witness_in_seconds", 30)
        delay_first = Config.get("subscriptions", "delay_to_next_witness_only_first", 4)

        for group in shuffled_per_group.keys():
            remaining_to_delay = delay_first

            for witness in shuffled_per_group[group]:
                witness_url = witness["url"] + Config.get("subscriptions", "postfix", default="/trigger")
                subscribed_witnesses_send = witness.get("whitelist_providers", None)
                if subscribed_witnesses_send is not None and\
                        unmasked_provider_info["name"] not in subscribed_witnesses_send:
                    logging.getLogger(__name__).debug(
                        "Sending to witness {0} was skipped, provider {1} not allowed ...".format(
                            witness_url,
                            unmasked_provider_info["name"]
                        )
                    )
                    continue

                success = False

                try:
                    response = self._do_post(witness_url, incident)
                    success = response and response.status_code == 200
                    errorMessage = "HTTP response " + str(response.status_code)
                except Exception as e:
                    errorMessage = str(e)

                if not success:
                    logging.getLogger(__name__).info(
                        "Sending to witness {0} has failed due to {1}, continueing ...".format(
                            witness_url,
                            errorMessage
                        )
                    )
                    subscribed_witnesses_status[witness_url] = errorMessage
                else:
                    logging.getLogger(__name__).debug(
                        "Sending to witness {0} was successfull".format(
                            witness_url
                        )
                    )
                    subscribed_witnesses_status[witness_url] = "ok"

                if delay_to_next > 0 and remaining_to_delay > 0 and len(shuffled_per_group[group]) > 1:
                    logging.getLogger(__name__).debug("Waiting before sending to next witness")
                    time.sleep(delay_to_next)
                    remaining_to_delay = remaining_to_delay - 1
        return subscribed_witnesses_status


class JsonProcessor(GenericProcessor):
    """ Simple json processor: takes input and returns it.
        Proper json format is expected
    """
    def __init__(self):
        super(JsonProcessor, self).__init__(file_ending=".json")

    def _process_source(self, source, source_type):
        if source_type == "file":
            content = io.open(source, encoding="utf-8").read()
            incident = utils.save_json_loads(content)
        else:
            incident = utils.save_json_loads(source)

        # it is assumed that this json already conforms to CommonFormat

        return incident

    def _incident_of_interest(self, incident):
        if self._debug_identifiers:
            info = json.dumps(incident["provider_info"])
            for ident in self._debug_identifiers:
                if ident in info:
                    return True
            return False
        return True
