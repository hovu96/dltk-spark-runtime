#!/bin/bash
set -e
set -x

# editor
docker build --rm -f "./editor/thin.Dockerfile" -t hovu96/dltk-spark-runtime:editor-thin .
docker push hovu96/dltk-spark-runtime:editor-thin

# driver
docker build --rm -f "./driver/thin.Dockerfile" -t hovu96/dltk-spark-runtime:driver-thin .
docker push hovu96/dltk-spark-runtime:driver-thin

# driver-proxy
docker build --rm -f "./driver-proxy/Dockerfile" -t hovu96/dltk-spark-runtime:driver-proxy .
docker push hovu96/dltk-spark-runtime:driver-proxy

# relay
docker build --rm -f "./relay/Dockerfile" -t hovu96/dltk-spark-runtime:relay .
docker push hovu96/dltk-spark-runtime:relay
