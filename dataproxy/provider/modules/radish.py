from pprint import pprint
from time import sleep
import logging
import requests
import json
import os

from ..json.processor import GenericJsonProcessor
from ... import Config
from ... import utils
from ...app import get_push_receiver
from ...datestring import date_to_string

from bookiesports.normalize import IncidentsNormalizer, NotNormalizableException

"""
    Provider module, actual name obfuscated
"""
NAME = os.path.basename(__file__).split(".")[0]
PLAIN_NAME = Config.get("providers", NAME, "name", default=None)

EVENTS_HISTORY = {}


def _get(*args, **kwargs):
    return Config.get("providers", NAME, *args, **kwargs)


def logged_json_get(url):
    logging.getLogger(__name__).info("[GET] " + url)
    json_payload = utils.save_json_loads(
        requests.get(
            url,
            timeout=_get("timeout", 2)
        ).content
    )
    if "error" in json_payload:
        raise Exception(url + ": " + json_payload["error"])
    if "pager" in json_payload:
        # fetch all
        _paged_results = [json_payload]
        while _paged_results[len(_paged_results) - 1]["pager"]["per_page"] == len(_paged_results[len(_paged_results) - 1]["results"]):
            _url = url + "&page=" + str(_paged_results[len(_paged_results) - 1]["pager"]["page"] + 1)
            logging.getLogger(__name__).info("[GET] " + _url)
            # we received a full page, query next
            _paged_results.append(
                utils.save_json_loads(
                    requests.get(
                        _url,
                        timeout=_get("timeout", 2)
                    ).content
                )
            )
        # merge results
        json_payload = {
            "success": _paged_results[0]["success"],
            "pager": _paged_results[0]["pager"],
            "results": []
        }
        for _json_payload in _paged_results:
            for _event in _json_payload["results"]:
                json_payload["results"].append(_event)
    return json_payload


def _as_list(result, find_string=None):
    if "results" in result:
        result = result["results"]
    return [str(x["id"]) + " - " + x["name"] for x in result if find_string is None or find_string.lower() in x["name"].lower()]


def _unify(result, find_string=None):
    if "results" in result:
        result = result["results"]
    return [{
        "id": x["id"],
        "name": x["name"]
    } for x in result if find_string is None or find_string.lower() in x["name"].lower()]


def _parse_id(id_or_name):
    try:
        return int(id_or_name)
    except Exception:
        return None


def resolve_via_api(url):
    # resolve via http API
    seperator = "&" if "?" in url else "?"

    url_with_token = (url + seperator +
                      "token=" + _get("token"))
    return logged_json_get(url_with_token)


def _get_sports_to_track():
    return _unify(_get(
        "recognize",
        "sports"
    ))


def _id_to_sport(sport_id):
    sports = _get(
        "recognize",
        "sports"
    )
    return next(item for item in sports if str(item["id"]) == str(sport_id))


def _get_upcoming_events(api, sport_id, league_id):
    return resolve_via_api(_get("api", api, "upcoming") + "?sport_id=" + str(sport_id) + "&league_id=" + str(league_id))


def _get_inplay_events(api, sport_id, league_id):
    return resolve_via_api(_get("api", api, "inplay") + "?sport_id=" + str(sport_id) + "&league_id=" + str(league_id))


def _get_ended_events(api, sport_id, league_id):
    now = date_to_string()
    now = now[0:4] + now[5:7] + now[8:10]
    return resolve_via_api(
        _get("api", api, "ended") +
        "?sport_id=" + str(sport_id) +
        "&league_id=" + str(league_id) +
        "&day=" + now
    )


def _get_event(api, event_id):
    return resolve_via_api(_get("api", api, "event") + "?event_id=" + str(event_id))


def _get_event_result_betfair(event_id):
    return resolve_via_api(_get("api", "results") + "?event_id=" + event_id)


class Processor(GenericJsonProcessor):
    def __init__(self):
        super(Processor, self).__init__()
        self._sports_to_track = None

    def _parse_raw(self, raw_file_content, raw_environ=None):
        return raw_file_content

    def _get_provider_info(self, event):
        info = {
            "name": PLAIN_NAME,
            "pushed": date_to_string(),
            "api_event_id": event["id"]
        }
        if event.get("our_event_id", None) is not None:
            info["our_event_id"] = event["our_event_id"]
        return info

    def _process_event_json(self, source_json):
        incidents = []
        for _event in source_json["results"]:
            try:
                incident_id = {
                    "sport": _id_to_sport(_event["sport_id"])["name"],
                    "home": _event["home"]["name"],
                    "away": _event["away"]["name"],
                    "start_time": date_to_string(_event["time"]),
                    "event_group_name": _event["league"]["name"],
                }
            except KeyError:
                # wrong format?
                pass

            call = self._mapStatus(_event["time_status"])
            arguments = self._getArgument(call, incident_id)

            incidents.append({
                "id": incident_id,
                "call": call,
                "arguments": arguments,
                "provider_info": self._get_provider_info(_event)
            })

            if call == "finish":
                # get results
                event_details = _get_event_result_betfair(_event["id"])
                if event_details["success"] == 1:
                    result = event_details["results"][0]["ss"].split("-")
                    result = {
                        "home_score": result[0],
                        "away_score": result[1]
                    }
                    incidents.append({
                        "id": incident_id,
                        "call": "result",
                        "arguments": result,
                        "provider_info": self._get_provider_info(_event)
                    })

        # it's unfeasible to maintain a list of applicable leagues. We attempt normalization
        # and only forward incidents that can be normalized (witness will still re-normalize as
        # dataproxies normally don't do that
        normalized = []
        normalizer = IncidentsNormalizer(chain=_get("network"))
        for incident in incidents:
            try:
                normalized.append(normalizer.normalize(incident, True))
            except NotNormalizableException as e:
                logging.getLogger(__name__).debug(str(e.__class__.__name__) + ": " + str(incident["id"]))
                pass
        return normalized

    def _process_source(self, source, source_type):
        if source_type == "string":
            source = json.loads(source)

        if "results" in source and "home" in source["results"][0]:
            return self._process_event_json(source)

        return []

    def source_of_interest(self, source):
        if source:
            return '"results": [{"id":' in source and "sport_id" in source and "league" in source and "home" in source and "away" in source
        else:
            return False

    def _mapStatus(self, status):
        status_map = {
            "0": "create",  # Not Started,
            "1": "in_progress",  # InPlay
            "3": "finished",  # Ended
            "4": "canceled",  # Postponed
            "5": "canceled",  # Cancelled
            "8": "canceled",  # Abandoned
            "9": "canceled",  # Retired
            "99": "canceled"  # Removed
        }
        return status_map.get(status, "canceled")

    def _getArgument(self, call, incident_id):
        if call == "create":
            return {
                "season": incident_id["start_time"][0:4]
            }
        elif call == "in_progress":
            return {
                "whistle_start_time": None
            }
        elif call == "finished":
            return {
                "whistle_end_time": None
            }
        else:
            return {"reason": None}

    def _incident_of_interest(self, incident):
        # check history, make sure it doesnt explode
        giid = incident["unique_string"] + incident["provider_info"]["name"]
        found = giid in EVENTS_HISTORY
        if not found:
            keys = list(EVENTS_HISTORY.keys())
            if len(keys) > 1000:
                EVENTS_HISTORY.pop(keys[0])
            EVENTS_HISTORY[giid] = True
        return not found


class BackgroundThread():

    def getName(self):
        return "BackgroundPoller_" + PLAIN_NAME

    def _send(self, results):
        try:
            response = requests.post(
                "http://localhost:" + str(Config.get("wsgi", "port")) + "/push/" + NAME,
                files={"json": json.dumps(results)}
            )
            if response and response.content == _get("processor", "response").encode() and response.status_code == 200:
                pass
            else:
                raise Exception("Pull could not be pushed, response " + str(response.status_code))
        except Exception as e:
            logging.getLogger(self.getName()).warning("Fetching events failed ... error below ... continueing with next")
            logging.getLogger(self.getName()).exception(e)

    def execute(self):
        api = "general"
        leagues = _get(
            "recognize",
            "leagues"
        )
        for league in leagues:
            #self._send(_get_upcoming_events(api, league["sport_id"], league["id"]))
            #self._send(_get_inplay_events(api, league["sport_id"], league["id"]))
            self._send(_get_ended_events(api, league["sport_id"], league["id"]))

    def run(self):
        sleep(2)
        while True:
            try:
                logging.getLogger(self.getName()).info("Fetching events ...")
                self.execute()
                sleep(
                    _get(
                        "polling_in_seconds",
                        5
                    )
                )
            except Exception as e:
                logging.getLogger(self.getName()).info("Exception while fetching events.")
                logging.getLogger(self.getName()).exception(e)
                # sleep additionally
                sleep(
                    _get(
                        "polling_in_seconds",
                        5
                    )
                )


class TooManyFoundException(Exception):
    pass


class NoneFoundException(Exception):
    pass


class CommandLineInterface():

    def _get_leagues(self, sport_id, country_id=None):
        _url = _get("api", "leagues") + "?sport_id=" + str(sport_id)
        if country_id is not None:
            _url = _url + "&cc=" + country_id
        return resolve_via_api(_url)

    def pull(self, sport_id, league_id, date_from, date_to, matches=None, details=None):
        _pulled = _get_upcoming_events(sport_id, league_id)
        _processor = Processor()
        if matches is not None:
            _processor.debug_games(matches)
        incidents = []
        receiver = get_push_receiver(
            PLAIN_NAME,
            _processor
        )
        result = receiver.process_content(
            json.dumps(_pulled),
            ".json",
            target="skip",
            async_queue=False
        )
        for incident in result["incidents"]:
            incidents.append(incident)
        return incidents

    def find(self, sport, country, eventgroup, season, date_from, date_to, details):
        if sport is not None:

            if _parse_id(eventgroup) is None:
                return _as_list(self._get_leagues(sport, country), eventgroup)
            else:
                return resolve_via_api(_get("api", "general", "upcoming") + "?sport_id=" + sport)
        else:
            return _as_list(_get_sports_to_track())
