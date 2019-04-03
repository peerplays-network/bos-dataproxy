import falcon
import logging
import pkg_resources

from .stores import RawStore, ProcessedFileStore,\
    IncidentFileStore, CacheFileStore

from .routes.push import PushReceiver
from .provider.json.processor import GenericJsonProcessor
from . import Config
import threading
import os


def get_push_receiver(provider_name, provider_processor, provider_success_response=None, raw_store=None, processed_store=None, incident_store=None):
    if provider_success_response is None:
        provider_success_response = Config.get(provider_name, "processor", "response", default="RECEIVED_OK")
    if raw_store is None:
        raw_store = RawStore()
    if processed_store is None:
        processed_store = ProcessedFileStore()
    if incident_store is None:
        incident_store = IncidentFileStore()
    return PushReceiver(
        raw_store,
        processed_store,
        incident_store,
        provider_name,
        provider_success_response,
        provider_processor
    )


def create_app(raw_store, processed_store, incident_store):
    """
        Creates the Falcon app and adds routes to all providers
    """
    api = falcon.API()

    provider_config = Config.get("providers", default={})

    background_threads = []

    for key, value in provider_config.items():
        if "processor" in value and value["processor"]["type"] == "generic":
            _processor = GenericJsonProcessor(value["processor"].get("timezone", None))
        else:
            module_to_load = Config.get("providers", key, "module", default=key)
            try:
                module = __import__(module_to_load, fromlist=[module_to_load])
            except ModuleNotFoundError:
                module = __import__("dataproxy.provider.modules." + module_to_load, fromlist=[module_to_load])
            _class = getattr(module, "Processor")
            _processor = _class()

            # check if a thread is necessary
            try:
                _class = getattr(module, "BackgroundThread")
                _object = _class()
                background_threads.append(
                    threading.Thread(
                        name=_object.getName(),
                        target=_object.run
                    )
                )
            except AttributeError:
                pass

        # start all background threads
        for t in background_threads:
            logging.getLogger(__name__).info("Starting thread for {}".format(t))
            t.start()

        logging.getLogger(__name__).info("Loading " + _processor.__class__.__name__ + " for provider " + key)
        api.add_route(
            "/push/" + key,
            get_push_receiver(
                key,
                _processor,
                value["processor"].get("response", None),
                raw_store,
                processed_store,
                incident_store
            )
        )

    from .routes.isalive import IsAlive
    api.add_route("/isalive", IsAlive(incident_store, background_threads))

    from .routes.statistics import GetStatistics
    api.add_route("/statistics", GetStatistics())

    from .routes.replay import Replay
    api.add_route("/replay", Replay())

    return api


def get_app():
    """
        Initiates the stores and creates the falcon app
    """
    # check bookiesports versiom
    versions = {}
    for name in ["peerplays", "bookiesports"]:
        try:
            versions[name] = pkg_resources.require(name)[0].version
        except pkg_resources.DistributionNotFound:
            versions[name] = "not installed"

    if versions["bookiesports"].split(".")[0] == "0" and versions["bookiesports"].split(".")[1] == "0" and\
            int(versions["bookiesports"].split(".")[2]) < 25:
        raise Exception("Please upgrade your bookiesports version to >= 0.0.25 (pip3 install bookiesports --upgrade)")

    #     initialize stores or other modules that may be mocked for testing
    processed_store = ProcessedFileStore()
    incident_store = IncidentFileStore()
    raw_store = RawStore()
    app = create_app(raw_store, processed_store, incident_store)

    logging.getLogger(__name__).info("BOS dataproxy uses " + str(versions) + ", has been initialized and is listening to incoming pushes ...")

    return app
