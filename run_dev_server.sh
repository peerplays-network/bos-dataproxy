#!/usr/bin/env bash

source ./setup.sh

function run_dev_server {
	GUNICORN_CMD_ARGS="--bind=localhost:8010 --timeout 180" gunicorn --reload 'dataproxy.app:get_app()'
}

eval_in_virtual_environment run_dev_server