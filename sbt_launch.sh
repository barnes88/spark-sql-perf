#!/bin/bash
cd "$(dirname "$0")"
exec sbt "run `echo "$@"`"
