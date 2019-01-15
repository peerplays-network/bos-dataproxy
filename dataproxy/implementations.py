import logging

from .processors import JsonProcessor
from . import Config
from .stores import IncidentFileStore, RawStore, ProcessedFileStore
from .routes.push import PushReceiver

import json
from bos_incidents.exceptions import DuplicateIncidentException

import threading
import time
from strict_rfc3339 import InvalidRFC3339Error

from . import utils
from .utils import slugify
from datetime import timedelta
from bos_incidents.format import string_to_incident
from dataproxy.utils import CommonFormat


def _send_to_witness(processor, incident, target=None):
    try:
        initial_delay = Config.get("subscriptions",
                                   "delay_before_initial_sending_in_seconds",
                                   incident["call"],
                                   0)

        if initial_delay > 0:
            logging.getLogger(__name__).info("Incident " + incident["unique_string"] + ": Waiting before sending " + incident["call"])
            time.sleep(initial_delay)
            logging.getLogger(__name__).info("Incident " + incident["unique_string"] + ": Sending result now")

        PushReceiver.subscribed_witnesses_status = processor.send_to_witness(
            incident,
            target=target
        )
        received_witnesses = len([key for key, value in PushReceiver.subscribed_witnesses_status.items() if value == "ok"])
        logging.getLogger(__name__).debug("Incident " + incident["unique_string"] + ": Successfully sent to " + str(received_witnesses) + " witnesses")
        return received_witnesses
    except Exception as e:
        logging.getLogger(__name__).info("Incident " + incident["unique_string"] + ": PUSH to witness failed, continueing anyways, exception below")
        logging.getLogger(__name__).exception(e)


def _send_list_to_witness(processor, incident_list, target=None, async_queue=True):
    for incident in incident_list:
        logging.getLogger(__name__).info("Trigger sending " + incident["unique_string"])

        if async_queue:
            # send to witnesses
            thr = threading.Thread(target=_send_to_witness,
                                   args=(processor, incident, target,))
            thr.start()  # we dont care when it finishes
        else:
            _send_to_witness(processor, incident, target=target)


def process_content(provider_name,
                    processor,
                    processed_store,
                    incident_store,
                    incident_database,
                    file_content,
                    file_ending,
                    restrict_witness_group=None,
                    async_queue=True):
    file_name = None

    # before storing, check if its worth processing
    is_interesting = file_content is not None
    if is_interesting and processor:
        is_interesting = processor.source_of_interest(
            file_content
        )
    incidents = []
    do_not_send_to_witness = True
    if is_interesting:
        # store found file again
        file_name = processed_store.save(
            provider_name,
            file_content,
            file_ext=file_ending)
        try:
            # process content (should be asynchronous)
            if processor:
                for incident in processor.process(file_content):
                    logging.getLogger(__name__ + "_" + provider_name).debug("Postprocessing " + incident["unique_string"])
                    incident["provider_info"]["source_file"] = file_name
                    incidents.append(incident)
                    # only send if its a new incident
                    logging.getLogger(__name__ + "_" + provider_name).debug(" ... exists")
                    do_not_send_to_witness = incident_store.exists(
                        provider_name,
                        file_ext=".json",
                        file_name=incident["unique_string"])

                    if not do_not_send_to_witness:
                        logging.getLogger(__name__ + "_" + provider_name).debug(" ... save in incidents folder")
                        # save locally
                        incident_file = incident_store.save(
                            provider_name,
                            json.dumps(incident),
                            file_ext=".json",
                            file_name=incident["unique_string"])
                        try:
                            logging.getLogger(__name__ + "_" + provider_name).debug(" ... save in incidents database")
                            incident_database.insert_incident(incident)
                        except DuplicateIncidentException:
                            pass
                        except Exception as e:
                            logging.getLogger(__name__ + "_" + provider_name).info(provider_name + ": INSERT INTO stats failed, continueing anyways, incident file is " + incident_file + "m exception below")
                            logging.getLogger(__name__ + "_" + provider_name).exception(e)
                        incident.pop("_id", None)
                        try:
                            logging.getLogger(__name__ + "_" + provider_name).debug(" ... sending to witnesses (" + str(restrict_witness_group) + ", async_queue=" + str(async_queue) + ")")
                            if async_queue:
                                # send to witnesses
                                thr = threading.Thread(target=_send_to_witness,
                                                       args=(processor, incident, restrict_witness_group,))
                                thr.start()  # we dont care when it finishes
                            else:
                                _send_to_witness(processor, incident, restrict_witness_group=restrict_witness_group)
                        except Exception as e:
                            logging.getLogger(__name__ + "_" + provider_name).info(provider_name + ": PUSH to witness failed, continueing anyways, incident file is " + incident_file + "m exception below")
                            logging.getLogger(__name__ + "_" + provider_name).exception(e)
        except Exception as e:
            logging.getLogger(__name__ + "_" + provider_name).info(provider_name + ": Processing failed, continueing anyways. Source file is " + file_name + ", exception below")
            logging.getLogger(__name__ + "_" + provider_name).exception(e)

    return {
        "file_name": file_name,
        "amount_incidents": len(incidents),
        "incidents": incidents,
        "do_not_send_to_witness": do_not_send_to_witness,
        "is_interesting": is_interesting
    }


def replay(restrict_witness_group=None,
           providers=None,
           received=None,
           processor=None,
           name_filter=None,
           incidents=None,
           async_execution=None,
           async_queue=None,
           only_report=None,
           target=None):
    if name_filter is None and (incidents is None or incidents == []):
        report = {"name_filter": "Name filter must not be empty"}
        return report
    if async_execution is None:
        async_execution = False
    if only_report is None:
        only_report = False

    logging.getLogger(__name__).info("Replay: Collecting configuration ...")

    replay_stats = {}
    replay_stats["async_execution"] = async_execution
    replay_stats["restrict_witness_group"] = restrict_witness_group
    replay_stats["target"] = target

    if providers is None:
        providers = list(Config.get("providers").keys())
    if type(providers) == str:
        providers = [providers]

    replay_stats["providers"] = providers

    if processor is None:
        processor = JsonProcessor()

    replay_stats["processor"] = processor.__class__.__name__

    if incidents is None:
        incidents = []

        if type(name_filter) == str:
            name_filter = name_filter.split(",")

        if name_filter is not None:
            offset_left = 3
            offset_right = 3
            match_date = None
            for tmp in name_filter:
                tmp = slugify(tmp)
                try:
                    match_date = utils.string_to_date(tmp[0:20])
                    break
                except InvalidRFC3339Error:
                    pass
                try:
                    match_date = utils.string_to_date(tmp[0:8])
                    break
                except InvalidRFC3339Error:
                    pass
                try:
                    match_date = utils.string_to_date(tmp[0:10])
                    break
                except InvalidRFC3339Error:
                    pass
            if "create" in name_filter:
                offset_left = 28
            if match_date and received is None:
                received = []
                for i in range(-offset_left, offset_right):
                    _date = utils.date_to_string(match_date + timedelta(days=i))
                    received.append(_date[0:4] + _date[5:7] + _date[8:10])

        folder_filter = []
        for provider in providers:
            folder_filter.append(provider)
        if received is None:
            received = "2018"
        if type(received) == str:
            received = [received]
        for tmp in received:
            folder_filter.append(tmp)

        replay_stats["folder_filter"] = folder_filter

        replay_stats["name_filter"] = name_filter

        logging.getLogger(__name__).info("Replay: Finding all incidents with configuration " + str(replay_stats))
        for incident in processor.process_generic(
                folder="dump/d_incidents",
                folder_filter=folder_filter,
                name_filter=name_filter):
            incidents.append(incident)
    else:
        if type(incidents) == str:
            incidents = [incidents]
        if type(incidents) == list and len(incidents) > 0 and type(incidents[0]) == str:
            manufactured = []
            for item in incidents:
                for provider in providers:
                    manufactured.append(string_to_incident(item, provider_info=provider))
            incidents = manufactured

    replay_stats["amount_incidents"] = len(incidents)
    incident_ids = []
    for incident in incidents:
        incident_ids.append(incident["unique_string"])

    if replay_stats["amount_incidents"] == 1:
        replay_stats["incidents"] = incidents
    else:
        replay_stats["incidents"] = incident_ids

    logging.getLogger(__name__).info("Found " + str(len(incident_ids)) + " incidents.")

    match_found = True
    for witness in Config.get("subscriptions", "witnesses"):
        if restrict_witness_group is not None:
            match_found = restrict_witness_group == witness["group"]

        if target is not None:
            match_witness = (
                target == witness["url"] or
                target == witness.get("name", None)
            )
            match_found = match_found and match_witness
        if match_found:
            break

    if not match_found:
        logging.getLogger(__name__).info("Replay: No matching witnesses found")
        replay_stats["any_matched_witnesses"] = False
        return replay_stats

    replay_stats["any_matched_witnesses"] = True

    if not only_report:
        sorted_list = sorted(incidents, key=lambda k: k['provider_info']['pushed'])
        logging.getLogger(__name__).info("Replay: Sorted " + str(len(sorted_list)) + " incidents ...")

        if restrict_witness_group is not None or target is not None:
            target = {
                "chain": restrict_witness_group,
                "witness": target
            }

        if async_execution:
            # send to witnesses
            thr = threading.Thread(target=_send_list_to_witness,
                                   args=(processor, sorted_list, target, async_queue))
            thr.start()  # we dont care when it finishes
            replay_stats["incidents_sent"] = True
        else:
            number = _send_list_to_witness(processor,
                                           sorted_list,
                                           target=target,
                                           async_queue=async_queue)
            replay_stats["incidents_sent"] = number
    else:
        replay_stats["incidents_sent"] = False
    return replay_stats
