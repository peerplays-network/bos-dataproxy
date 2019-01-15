import json
import falcon
import logging

from bos_incidents import factory


class GetStatistics(object):
    """ Statistics for all found providers
    """

    def on_get(self, req, resp):
        resp.body = self.get_statistics_string()
        resp.status = falcon.HTTP_200
        logging.getLogger(__name__).info("GET statistics received from " + req.remote_addr)

    def sync_storage(self, storage):
        from .. import utils
        from ..processors import JsonProcessor

        parser = JsonProcessor()

        providers = list(Config.get("providers", default={}).keys())
        providers.append("2018")
        providers.append("2019")

        for incident in parser.process_generic(
                folder="dump/d_incidents",
                folder_filter=providers,
                name_filter="soccer"
        ):
            if not storage.incident_exists(incident):
                # start beginning of april
                if incident["id"]["start_time"].startswith("2018-05"):
                    storage.insert_incident(incident)
                    logging.getLogger(__name__).info("Inserting " + str(incident))

            _incident_date = utils.string_to_date(incident["provider_info"]["pushed"])
#             if not last_pushed:
#                 last_pushed = _incident_date
#             else:
#                 if last_pushed < _incident_date:
#                     last_pushed = _incident_date
#         storage.set_status({"last_inserted": last_pushed})

    def get_statistics(self, storage=None):
        if storage is None:
            storage = factory.get_incident_storage()
        print(storage._mongodb_config)

        events = storage.get_events()
        providers = []
        for event in events:
            for incident in storage.get_incidents_from_event(event):
                if incident["provider_info"]["name"] not in providers:
                    providers.append(incident["provider_info"]["name"])

        return_dict = {}
        return_dict["providers"] = providers
        return_dict["storage_information"] = storage._mongodb_config

        return_dict["events"] = events

        return json.dumps(return_dict)

    def get_statistics_string(self, storage=None):
        if storage is None:
            storage = factory.get_incident_storage()
        print(storage._mongodb_config)

        events = storage.get_events()
        providers = []
        for idx, event in enumerate(events):
            for incident in storage.get_incidents_from_event(event):
                if incident["provider_info"]["name"] not in providers:
                    providers.append(incident["provider_info"]["name"])
        providers = sorted(providers)

        buffer = ";".join(storage._collection_names.values()) + "\n"

        def list_all(event, call, buffer, providers, one_line=None):
            if one_line is None:
                if event.get(call, None) is not None:
                    buffer = buffer + ("    " + call)
                    if event[call].get("status", None) is not None:
                        buffer = buffer + ("  " + json.dumps(event[call]["status"])) + "\n"
                    if event[call].get("incidents", None) is not None:
                        for incident in event[call]["incidents"]:
                            buffer = buffer + ("        " + incident["provider_info"]["pushed"] + " - " + incident["provider_info"]["name"] + " - " + json.dumps(incident["arguments"])) + "\n"
            else:
                providers_found = []
                for unused in providers:
                    providers_found.append("-")
                if event.get(call, None) is not None:
                    if event[call].get("incidents", None) is not None:
                        for idx, provider in enumerate(providers):
                            for incident in event[call]["incidents"]:
                                if provider in incident["provider_info"]["name"]:
                                    providers_found[idx] = "x"
                buffer = buffer + one_line + " " + "".join(providers_found) + " "
            return buffer
#         for idx, event in enumerate(events):
#             buffer = buffer + (str(idx) + ": " + event["id_string"]) + "\n"
        buffer = buffer + ("Providers: " + " ".join(providers) + "\n")
        buffer = buffer + ("event number; create; in_progress; finish; result; \n")
        for idx, event in enumerate(events):
            buffer = buffer + (event["id_string"] + " ")
            buffer = list_all(event, "create", buffer, providers, ';')
            buffer = list_all(event, "in_progress", buffer, providers, ';')
            buffer = list_all(event, "finish", buffer, providers, ';')
            buffer = list_all(event, "result", buffer, providers, ';')
            buffer = buffer + ("\n")

        return buffer