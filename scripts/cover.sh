#!/usr/bin/env bash

set -e
echo "" > cover.out

for d in $(go list $@); do
    go test -coverprofile=profile.out $d
    if [ -f profile.out ]; then
        cat profile.out >> cover.out
        rm profile.out
    fi
done