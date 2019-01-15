#!/usr/bin/env bash

function eval_in_virtual_environment {
    VIRTUALENV_NAME=env

    if [ ! -d ${VIRTUALENV_NAME} ]; then
      virtualenv ${VIRTUALENV_NAME} -p python3
    fi

    source ${VIRTUALENV_NAME}/bin/activate
    pip install --upgrade pip
    pip install --upgrade wheel
    pip install -r requirements.txt  --upgrade --upgrade-strategy eager
    deactivate
    source ${VIRTUALENV_NAME}/bin/activate
    echo "Running '$1' inside virtual environment..."
    $1
} 
