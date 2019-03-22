import click

from wsgiref import simple_server

from .app import get_app


@click.group()
def main():
    pass


@main.command()
@click.option("--host", default="127.0.0.1")
@click.option("--port", default=8000)
def wsgi(
    host,
    port
):
    httpd = simple_server.make_server(host, port, get_app())
    httpd.serve_forever()
