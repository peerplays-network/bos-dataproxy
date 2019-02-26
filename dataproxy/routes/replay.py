import json
import falcon
import logging
from pprint import pformat

from .resolve import get_params


class Replay(object):
    """ Replay message for the dataproxy
    """
    def on_get(self, req, resp):
        from .. import implementations, Config

        params = get_params(req,
                            "restrict_witness_group",
                            "provider",
                            "received",
                            "name_filter",
                            "only_report",
                            "token",
                            "manufacture",
                            "target")
        logging.getLogger(__name__).info("GET replay received (" + req.remote_addr + ", " + req.url + ")")

        try:
            if params.get("token") is None or not params.get("token") == Config.get("remote_control", "token"):
                resp.status = falcon.HTTP_404
                return
        except KeyError:
            resp.status = falcon.HTTP_404
            return

        params.pop("token")

        if any(params.values()):
            params["providers"] = params.pop("provider")
            params["incidents"] = params.pop("manufacture")
            report = implementations.replay(**params, async_execution=True)
            logging.getLogger(__name__).debug("GET replay done, report below\n" + pformat(report))
            resp.body = json.dumps(report)
            resp.content_type = falcon.MEDIA_JSON
        else:
            params["restrict_witness_group"] = "Deprecated, use target"
            params["target"] = "This can be a witness name, or witness group. Then replay is only sent to matched witnesses"
            params["provider"] = "Replay only using incidents from this provider, default from all providers"
            params["received"] = "Replay only incidents that were received matching this regex (e.g. 201805 to replay all received in May 2018). If not given it tries to guess the desired range according to the name_filter, if no guess poissble search complete database"
            params["name_filter"] = "Replay only incidents whose id matches this string or match all in comma seperated list (e.g. 'soccer', or '2018-05-31', or 'world  cup', or '2018-05-31,soccer,create'), mandatory."
            params["only_report"] = "Only generate a report of what would happen, default false"
            params["manufacture"] = "Unique string of an incident to be manufactured, format is specified in bos-incidents/format/incident_to_string"
            resp.body = json.dumps(
                {
                    "description": "Searches in the internal storage of the dataproxy for incidents that match the given parameters and replays them to the chosen witnesses",
                    "possible_arguments": params
                }
            )

        resp.status = falcon.HTTP_200
