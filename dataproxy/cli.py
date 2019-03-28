import click

from wsgiref import simple_server
from pprint import pprint

from .app import get_app
from . import Config
import logging


@click.group()
def main():
    pass


@main.command()
@click.option("--host", default=Config.get("wsgi", "host"))
@click.option("--port", default=Config.get("wsgi", "port"))
def wsgi(
    host,
    port
):
    logging.getLogger(__name__).info("Listening on " + host + ":" + str(port))
    httpd = simple_server.make_server(host, port, get_app())
    httpd.serve_forever()


def _load_module(provider):
    module = __import__(provider, fromlist=[provider])
    _class = getattr(module, "CommandLineInterface")
    return _class()


@main.command()
@click.argument("provider")
@click.option("--sport")
@click.option("--date_from")
@click.option("--date_to")
@click.option("--country")
@click.option("--eventgroup")
@click.option("--season")
@click.option("--details")
def find(
        provider,
        sport=None,
        eventgroup=None,
        season=None,
        country=None,
        date_from=None,
        date_to=None,
        details=False
):
    module = _load_module(provider)
    response = module.find(sport, country, eventgroup, season, date_from, date_to, details)
    pprint(response)


@main.command()
@click.argument("provider")
@click.option("--sport")
@click.option("--eventgroup")
@click.option("--date_from")
@click.option("--date_to")
@click.option("--matches")
def pull(
        provider,
        sport=None,
        eventgroup=None,
        date_from=None,
        date_to=None,
        matches=None,
        details=False
):
    module = _load_module(provider)
    response = module.pull(sport, eventgroup, date_from, date_to, matches, details)
    pprint(response)
