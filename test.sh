#!/bin/bash

set -euo pipefail

echo "It is currently $(date)."
if [ -v CUSTOM_DATE ]; then
    echo "An Acquisition date was provided by the environment."
fi
