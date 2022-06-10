#!/bin/bash
#
# Usage: bash install_env.sh

# Get parent path of current script
PARENT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
VENV_NAME="twitter_etl_repo"
VENV_INSTALL_REPO="~/venv/${VENV_NAME}"
REQUIREMENTS_TXT_FPATH="${PARENT_PATH}/requirements_gcp.txt"

# Check if directory exists, if not create venv
if [ ! -d $VENV_INSTALL_REPO ]; then
    python3 -m venv $VENV_INSTALL_REPO
    echo -e "Installed venv '${VENV_NAME}' to ${VENV_INSTALL_REPO}"
else
    echo -e "Venv '${VENV_NAME}' already exists in ${VENV_INSTALL_REPO}!"
fi

# Install python requirements
source "${VENV_INSTALL_REPO}/bin/activate" &&
    python -m pip install --upgrade pip &&
    python -m pip install -r "${REQUIREMENTS_TXT_FPATH}" &&
    echo -e "\nRequirements installed in => ${VENV_NAME}\n" &&
    deactivate
