import json
import falcon
import logging
import time
import pkg_resources
import hashlib
import glob
import os


from .. import datestring
from .. import Config, __VERSION__
from ..stores import IncidentFileStore
from .push import PushReceiver
from ..utils import CommonFormat
from ..processors import GenericProcessor
import datetime


class IsAlive(object):
    """ IsAlive message for the dataproxy

        Adds */isalive route which gives basic status of the dataproxy. The response has a json body that has three main contents:
        - status: String flag either "ok" or "nok", general state of the proxy
        - subscribers: List of dictionaries containing information of each subscriber. Contains a status flag as well
        - providers: List of dictionaries containing information of each provider. Contains a status flag as well

        This isalive can be called from localhost and from anywhere. No identifiable information on providers or subscribers is published when queried from anywhere. Details are added when it is called from localhost.
    """

    def __init__(self,
                 incidents_store):
        self._incidents_store = incidents_store

    def on_get(self, req, resp):
        resp.body = json.dumps(self.get_is_alive_message(req.remote_addr != "127.0.0.1"))
        resp.status = falcon.HTTP_200
        resp.content_type = "application/json"
        logging.getLogger(__name__).info("GET isalive received from " + req.remote_addr)

    def _mask_ip_in_message(self, witness_url, status):
        try:
            status = status.replace(witness_url, "*hidden*:*")
            status = status.replace(witness_url.split(":")[0], "*hidden*")
        except Exception:
            pass
        try:
            status = status.replace(witness_url.split("http://")[1].split(":")[0], "*hidden*")
        except Exception:
            pass
        try:
            status = status.replace(witness_url.split("https://")[1].split(":")[0], "*hidden*")
        except Exception:
            pass
        try:
            status = status.replace(witness_url.split("http://")[1].split("/")[0], "*hidden*")
        except Exception:
            pass
        try:
            status = status.replace(witness_url.split("https://")[1].split("/")[0], "*hidden*")
        except Exception:
            pass
        return status

    def get_is_alive_message(self, mask_names=True):
        all_is_well = True

        subscribed_witnesses_status = {}
        subscribed_witnesses_status_flag = "ok"
        shuffled_per_group = GenericProcessor.get_timed_shuffled_subscribers()
        for group in shuffled_per_group.keys():
            subscribed_witnesses_status[group] = []
            for witness in shuffled_per_group[group]:
                status_dict = {
                    "status": PushReceiver.subscribed_witnesses_status.get(witness["url"], "unknown")
                }
                if not mask_names:
                    status_dict["config"] = witness
                if status_dict["status"] != "ok":
                    subscribed_witnesses_status[group].append(status_dict)
                if status_dict["status"] != "ok" and status_dict["status"] != "unknown":
                    subscribed_witnesses_status_flag = "nok"
        keys = list(subscribed_witnesses_status.keys())
        for group in keys:
            if not subscribed_witnesses_status[group] or subscribed_witnesses_status[group] == []:
                subscribed_witnesses_status.pop(group)
        provider_names = Config.get("providers", {}).keys()
        provider_status = []
        mask = Config.get("subscriptions", "mask_providers", default=True)
        if mask:
            if CommonFormat.MASK is None:
                if type(mask) == bool:
                    mask = json.dumps(Config.get("subscriptions", "witnesses")) + json.dumps(Config.get("providers"))
                CommonFormat.MASK = mask
        for provider in provider_names:
            if self._incidents_store.folder_exists(provider):
                list_of_files = glob.glob(os.path.join(self._incidents_store.get_storage_path(provider), "*"))
                if list_of_files:
                    latest_file = max(list_of_files, key=os.path.getctime)
                    latest_ctime = os.path.getctime(latest_file)
                    print(datetime.datetime.now().timestamp() - latest_ctime)
                    if datetime.datetime.now().timestamp() - latest_ctime < Config.get("providers_setting", "error_after_no_incident_in_hours", 24) * 60 * 60:
                        status = "ok"
                    else:
                        status = "nok"
                        all_is_well = False
                    provider_dict = {
                        "status": status,
                        "last_incident": datestring.date_to_string(os.path.getctime(latest_file))
                    }
                else:
                    provider_dict = {
                        "status": "nok",
                        "last_incident": None
                    }
                    all_is_well = False
                    latest_file = None

                masked_name = provider + CommonFormat.MASK
                masked_name = hashlib.md5(masked_name.encode()).hexdigest()
                if mask_names:
                    provider_dict["name"] = masked_name
                else:
                    provider_dict["name"] = provider
                    provider_dict["hash"] = masked_name
                    provider_dict["last_incident_name"] = latest_file

                provider_status.append(provider_dict)
            else:
                status = "nok"
        if all_is_well:
            all_is_well = "ok"
        else:
            all_is_well = "nok"

        last_incident = ""
        if IncidentFileStore.last_written:
            last_incident = str(IncidentFileStore.last_written)
        message = {"status": all_is_well,
                   "subscribers": {
                       "status": subscribed_witnesses_status_flag,
                       "details": subscribed_witnesses_status
                   },
                   "providers": provider_status,
                   "last_written": last_incident}

        versions = {"dataproxy": __VERSION__}
        for name in ["peerplays", "bookiesports"]:
            try:
                versions[name] = pkg_resources.require(name)[0].version
            except pkg_resources.DistributionNotFound:
                versions[name] = "not installed"
        message["versions"] = versions

        return message
