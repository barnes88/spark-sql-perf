#!/bin/bash
pushd "$(dirname "$0")"
exec sbt "run `echo "$@"`"
popd
