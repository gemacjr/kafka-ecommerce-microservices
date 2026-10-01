#!/bin/sh
# Licensed to the Apache Software Foundation (ASF)
# Maven Wrapper Script for Unix/Mac

set -e

# Determine the script directory
MAVEN_PROJECTBASEDIR="${MAVEN_BASEDIR:-}"
if [ -z "$MAVEN_PROJECTBASEDIR" ]; then
    MAVEN_PROJECTBASEDIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
fi

MAVEN_WRAPPER_PROPERTIES="$MAVEN_PROJECTBASEDIR/.mvn/wrapper/maven-wrapper.properties"
DISTRIBUTION_URL=$(grep "distributionUrl" "$MAVEN_WRAPPER_PROPERTIES" | cut -d= -f2-)
MAVEN_HOME="$HOME/.m2/wrapper/dists"

# Extract the Maven directory name from the URL
MAVEN_DIST_NAME=$(basename "$DISTRIBUTION_URL" .zip)
MAVEN_INSTALL_DIR="$MAVEN_HOME/$MAVEN_DIST_NAME"

if [ ! -d "$MAVEN_INSTALL_DIR" ]; then
    echo "Downloading Maven $MAVEN_DIST_NAME..."
    mkdir -p "$MAVEN_INSTALL_DIR"
    TEMP_DIR=$(mktemp -d)
    TEMP_ZIP="$TEMP_DIR/maven.zip"
    curl -fsSL "$DISTRIBUTION_URL" -o "$TEMP_ZIP"
    unzip -q "$TEMP_ZIP" -d "$TEMP_DIR"
    mv "$TEMP_DIR"/*/ "$MAVEN_INSTALL_DIR/"
    rm -rf "$TEMP_DIR"
    echo "Maven downloaded to $MAVEN_INSTALL_DIR"
fi

# Find the mvn binary
MVN_CMD=$(find "$MAVEN_INSTALL_DIR" -name "mvn" -type f | head -1)

if [ -z "$MVN_CMD" ]; then
    echo "ERROR: Could not find mvn binary in $MAVEN_INSTALL_DIR"
    exit 1
fi

exec "$MVN_CMD" "$@"
