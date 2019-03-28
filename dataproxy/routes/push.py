import io
import cgi
import time

import pkg_resources
import falcon
import urllib

import logging


ALLOWED_FILE_TYPES = (
    "multipart/form-data"
)

ALLOWED_PUSHER = (
    "127.0.0.1",
    "79.125.109.208",  # (SpoCoSy1)
    "79.125.117.234",  # (SpoCoSy3)
    "79.125.117.129",  # (SpoCoSy4)
    "79.125.119.58",  # (SpoCoSy5)
    "79.125.119.88",  # (SpoCoSy6)
    "46.51.190.90",  # (Spocosy7)
    "213.95.40.4"  # Blockchain Projects BV
)


def validate_pusher(req, resp, resource, params):
    """ Only allow pushes from ALLOWED_PUSHER adresses """
    if req.remote_addr not in ALLOWED_PUSHER:
        msg = 'This remote address is not white listed'
        logging.getLogger(__name__).warning(
            "Request denied, remote address " +
            req.remote_addr + " not white listed"
        )
        raise falcon.HTTPBadRequest('Bad request', msg)


def validate_file_type(req, resp, resource, params):
    if req.content_type and req.content_type not in ALLOWED_FILE_TYPES:
        msg = 'File type not allowed. Must be XML'
        raise falcon.HTTPBadRequest('Bad request', msg)


class PushReceiver(object):
    """ Generic route that listens to file pushes from providers

        Every POST request is being analyzed, and if it contains interesting
        data it is stored and processed by the provider processor
    """

    subscribed_witnesses_status = {}

    def __init__(self,
                 raw_store,
                 processed_store,
                 incidents_store,
                 provider_name,
                 post_string_response,
                 provider_processor):
        self._provider_name = provider_name
        self._post_response = post_string_response
        self._processed_store = processed_store
        self._incidents_store = incidents_store
        self._raw_store = raw_store
        self._provider_processor = provider_processor

    def on_get(self, req, resp):
        resp.body = "Waiting for POST push notifications from " +\
                    self._provider_name
        resp.status = falcon.HTTP_200
        logging.getLogger(__name__ + "_" + self._provider_name).info("GET received from " + req.remote_addr)

    def on_pull(self, req, resp):
        resp.body = "Waiting for POST push notifications from " +\
                    self._provider_name
        resp.status = falcon.HTTP_200
        logging.getLogger(__name__ + "_" + self._provider_name).info("PULL received from " + req.remote_addr)

    def _cgiFieldStorageToDict(self, fieldStorage):
        """ Get a plain dictionary, rather than the '.value' system used
            by the cgi module.
        """
        params = {}
        for key in fieldStorage.keys():
            params[key] = fieldStorage[key].value
        return params

    @falcon.before(validate_pusher)
    def on_post(self, req, resp, raw_file_content=None):
        self.process(req, resp, raw_file_content=raw_file_content)

    def process_content(self, file_content, file_ending, restrict_witness_group=None, async_queue=True, target=None):
        from .. import implementations

        if restrict_witness_group is None and target is not None:
            restrict_witness_group = target

        return implementations.process_content(
            self._provider_name,
            self._provider_processor,
            self._processed_store,
            self._incidents_store,
            file_content,
            file_ending,
            restrict_witness_group=restrict_witness_group,
            async_queue=async_queue
        )

    def process(self, req, resp, raw_file_content=None):
        from .. import utils

        if raw_file_content is None:
            raw_file_content = utils.save_string(req.stream.read())
        raw_file_name = self._raw_store.save(
            self._provider_name,
            raw_file_content
        )
        file_content = None

        file_ending = None
        # depending on the content_type, search for contained files
        if "multipart/form-data" in req.content_type:
            try:
                upload = cgi.FieldStorage(
                    fp=io.BytesIO(raw_file_content.encode()),
                    environ=req.env)
                upload = self._cgiFieldStorageToDict(upload)
                try:
                    # check for xml file
                    file_content = upload["xml"]
                    if isinstance(file_content, bytes):
                        file_content = file_content.decode("utf-8")
                    if not isinstance(file_content, str):
                        raise falcon.HTTPBadRequest(
                            "Bad request",
                            "Sent file does not have any content")
                    file_ending = ".xml"
                except KeyError as e:
                    pass
                except Exception as e:
                    logging.getLogger(__name__).warning("Exception occured during processing, continueing anyways ...")
                    logging.getLogger(__name__).exception(e)

                if not file_content:
                    try:
                        file_content = upload["json"]
                        if isinstance(file_content, bytes):
                            file_content = file_content.decode("utf-8")
                        if not isinstance(file_content, str):
                            raise falcon.HTTPBadRequest(
                                "Bad request",
                                "Sent file does not have any content")
                        file_ending = ".json"
                    except Exception as e:
                        logging.getLogger(__name__).warning("Exception occured during processing, continueing anyways ...")
                        logging.getLogger(__name__).exception(e)
            except ValueError as e:
                logging.getLogger(__name__).warning("Exception occured during processing, continueing anyways ...")
                logging.getLogger(__name__).exception(e)

        elif "application/x-www-form-urlencoded" in req.content_type:
            file_content = urllib.parse.unquote(raw_file_content)
            if file_content.startswith("xml="):
                file_content = file_content[4:len(file_content)]
                file_ending = ".xml"
            elif file_content.startswith("json="):
                file_content = file_content[5:len(file_content)]
                file_ending = ".json"
            else:
                file_content = file_content
                file_ending = ".json"

        elif "application/json" in req.content_type:
            file_content = urllib.parse.unquote(raw_file_content)
            if file_content.startswith("xml="):
                file_content = file_content[4:len(file_content)]
                file_ending = ".xml"
            elif file_content.startswith("json="):
                file_content = file_content[5:len(file_content)]
                file_ending = ".json"
            else:
                file_content = file_content
                file_ending = ".json"

        result = self.process_content(file_content, file_ending)

        do_not_send_to_witness = result["do_not_send_to_witness"]
        is_interesting = result["is_interesting"]
        file_name = result["file_name"]
        amount_incidents = result["amount_incidents"]

        # no content given
        if not file_content:
            resp.body = "NO_CONTENT_GIVEN"
            resp.status = falcon.HTTP_400
        else:
            if is_interesting and not do_not_send_to_witness:
                # everthing worked, positive response
                resp.body = self._post_response
                resp.status = falcon.HTTP_200
            else:
                resp.body = self._post_response
                resp.status = falcon.HTTP_200

        # construct log message
        if not is_interesting:
            file_info = ', file content not interesting, skip'
        else:
            if file_name:
                file_info = ", " + file_name +\
                    " parsed, " + str(amount_incidents) + " incidents found and triggered sending to witnesses"
            else:
                file_info = ", " + raw_file_name + " accepted, nothing found"

        logging.getLogger(__name__ + "_" + self._provider_name).info(req.remote_addr + "/" + self._provider_name +
                                                                     ": POST received" + file_info)
