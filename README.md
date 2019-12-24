# BOS dataproxy

This software is provided under the copyright/licence stated in LICENSE.

The dataproxy is a universal middleman to be put inbetween data provider
and witnesses. Collects and reformats data given by providers into a unified json object,
as defined in dataproxy/dataproxy-schema.json and normalized by bookiesports.

If necessary, every provider has a service that somehow connects with the
providers API (via repeated pulls, message queue or similar) and creates
pushes to the dataproxy.

## Prepare server

The dataproxy is utilizing the filesystem and a MondoDB instance for achiving. Please install
before running the dataproxy. Gunicorn Python web server is used app the application.

```
apt install gunicorn mongodb-server
```

Installing Python Virtual Environement is required to run the application

```
apt -y intall virtualenv
```

All in one command-line

```
apt intall -y gunicorn mongodb-server virtualenv python3 git htop mosh
```

## Development use

Please note that the installation steps can change since this software is work in progress.

`setup.sh`
	Ensures that the given command is executed within a virtual environment

`run_dev_server.sh`
	Starts the dataproxy wsgi and listens to pushes. Automatic code reloading is enabled.

provider_service.sh provider_name command
	Starts the provider service, see provider_service.py and section Provider services.

Python runs in a virtual environment that is activated in all scripts via setup.s

### Example

To run the dev server locally, start
	run_dev_server.sh
When the server is up and running, initiate one of the providers, via
	provider_service.sh provider_name run_here
