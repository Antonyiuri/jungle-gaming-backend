#!/bin/sh
set -eu

if [ -f /runtime/runtime.env ]; then
    set -a
    . /runtime/runtime.env
    set +a
fi

exec /app/server
