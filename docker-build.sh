#!/usr/bin/env bash

set -euo pipefail
set -x

if [ -f release.properties ]; then
    VERSION=$(grep 'project.rel.pl.net.was\\:trino-cloud=' release.properties | cut -d'=' -f2)
else
    VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
fi
TAG=nineinchnick/trino-cloud:$VERSION

docker build \
    -t "$TAG" \
    --build-arg VERSION="$VERSION" \
    .

docker push "$TAG"
