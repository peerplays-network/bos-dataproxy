import falcon
import logging
import pkg_resources

from .stores import RawStore, ProcessedFileStore,\
    IncidentFileStore, CacheFileStore
    
from .routes.push import PushReceiver
from .provider.json.processor import GenericJsonProcessor
from . import Config


def get_push_receiver(provider_name, provider_processor, provider_success_response, raw_store, processed_store, incident_store):
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

    for key, value in provider_config.items():
        if "processor" in value and value["processor"]["type"] == "generic":
            _processor = GenericJsonProcessor(value["processor"].get("timezone", None))
        else:
            # search locally for the processor in module <key>.py
            module = __import__("", fromlist=[key])
            class_ = getattr(module, "Processor")
            _processor = _class()
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
    api.add_route("/isalive", IsAlive(incident_store))

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
