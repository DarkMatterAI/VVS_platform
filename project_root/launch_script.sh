#!/bin/bash

CONFIG_FILE="launch_config.yaml"
META_CONFIG_FILE="launch_config_meta.yaml"
BACKEND_APP_DIR="./backend/app"
INTEGRATION_TEST_DIR="./test_executor/src"

get_enabled_profiles() {
    # Get enabled services into an array
    readarray -t enabled_services < <(yq e '.plugins[] | select(.enabled == True) | path | .[1]' "$CONFIG_FILE")
    
    # For each enabled service, get its profile from meta config
    for service in "${enabled_services[@]}"; do
        yq e ".services.$service.profile" "$META_CONFIG_FILE"
    done
}

build_docker_compose_command() {
    local profiles=$(get_enabled_profiles)
    local cmd="docker compose"
    
    for profile in $profiles; do
        cmd+=" --profile $profile"
    done
    
    echo "$cmd"
}

copy_config_file() {
    cp "$CONFIG_FILE" "$BACKEND_APP_DIR/"
    cp "$META_CONFIG_FILE" "$BACKEND_APP_DIR/"
    echo "Copied config files to $BACKEND_APP_DIR/"
    
    cp "$CONFIG_FILE" "$INTEGRATION_TEST_DIR/"
    cp "$META_CONFIG_FILE" "$INTEGRATION_TEST_DIR/"
    echo "Copied config files to $INTEGRATION_TEST_DIR/"
}

if [ $# -eq 0 ]; then
    echo "Usage: $0 <docker-compose-command>"
    exit 1
fi

copy_config_file

docker_cmd=$(build_docker_compose_command)

$docker_cmd "$@"
