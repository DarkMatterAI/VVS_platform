#!/bin/bash

CONFIG_FILE="launch_config.yaml"
BACKEND_APP_DIR="./backend/app"
INTEGRATION_TEST_DIR="./test_executor/src"

get_enabled_profiles() {
    grep -A2 "^  [^:]\+:" "$CONFIG_FILE" | awk '
    /^  [^:]+:/ {service=$1}
    /enabled:/ {enabled=$2}
    /profile:/ {profile=$2}
    enabled=="True" {print profile}
    ' | sort | uniq | tr '\n' ' '
}

build_docker_compose_command() {
    local profiles=$(get_enabled_profiles)
    local cmd="docker compose"
    
    if [ ! -z "$profiles" ]; then
        for profile in $profiles; do
            cmd+=" --profile $profile"
        done
    fi
    
    echo "$cmd"
}

copy_config_file() {
    cp "$CONFIG_FILE" "$BACKEND_APP_DIR/"
    echo "Copied $CONFIG_FILE to $BACKEND_APP_DIR/"
    cp "$CONFIG_FILE" "$INTEGRATION_TEST_DIR/"
    echo "Copied $CONFIG_FILE to $INTEGRATION_TEST_DIR/"
}

if [ $# -eq 0 ]; then
    echo "Usage: $0 <docker-compose-command>"
    exit 1
fi

copy_config_file

docker_cmd=$(build_docker_compose_command)

$docker_cmd "$@"

