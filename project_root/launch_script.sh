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

check_integration_required() {
    # Get enabled services into an array
    readarray -t enabled_services < <(yq e '.plugins[] | select(.enabled == True) | path | .[1]' "$CONFIG_FILE")
    
    # Debug output
    echo "Enabled services: ${enabled_services[*]}" >&2
    
    count=0
    for service in "${enabled_services[@]}"; do
        requires_integration=$(yq e ".services.$service.requires_integration" "$META_CONFIG_FILE")
        # Debug output
        echo "Service $service requires integration: $requires_integration" >&2
        
        if [ "$requires_integration" = "True" ]; then
            ((count++))
        fi
    done
    echo $count
}

build_docker_compose_command() {
    local profiles=$(get_enabled_profiles)
    local cmd="docker compose"
    
    for profile in $profiles; do
        cmd+=" --profile $profile"
    done
    
    # Check if any enabled plugin requires integration
    local integration_count=$(check_integration_required)
    
    if [ "$integration_count" -gt 0 ]; then
        cmd+=" --profile plugin_integration_server"
    fi
    
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
# echo "Final docker command: $docker_cmd" >&2

$docker_cmd "$@"


# #!/bin/bash

# CONFIG_FILE="launch_config.yaml"
# META_CONFIG_FILE="launch_config_meta.yaml"
# BACKEND_APP_DIR="./backend/app"
# INTEGRATION_TEST_DIR="./test_executor/src"

# get_enabled_profiles() {
#     # Get enabled services
#     enabled_services=$(yq e '.plugins[] | select(.enabled == True) | path | .[1]' "$CONFIG_FILE")
    
#     # For each enabled service, get its profile from meta config
#     for service in $enabled_services; do
#         yq e ".services.$service.profile" "$META_CONFIG_FILE"
#     done
# }

# check_integration_required() {
#     # Get enabled services
#     enabled_services=$(yq e '.plugins[] | select(.enabled == True) | path | .[1]' "$CONFIG_FILE")
    
#     # Count enabled services that require integration
#     count=0
#     for service in $enabled_services; do
#         requires_integration=$(yq e ".services.$service.requires_integration" "$META_CONFIG_FILE")
#         if [ "$requires_integration" = "true" ]; then
#             ((count++))
#         fi
#     done
#     echo $count
# }

# build_docker_compose_command() {
#     local profiles=$(get_enabled_profiles)
#     local cmd="docker compose"
    
#     for profile in $profiles; do
#         cmd+=" --profile $profile"
#     done
    
#     # Check if any enabled plugin requires integration
#     if [ $(check_integration_required) -gt 0 ]; then
#         cmd+=" --profile plugin_integration_server"
#     fi
    
#     echo "$cmd"
# }

# copy_config_file() {
#     cp "$CONFIG_FILE" "$BACKEND_APP_DIR/"
#     cp "$META_CONFIG_FILE" "$BACKEND_APP_DIR/"
#     echo "Copied config files to $BACKEND_APP_DIR/"
    
#     cp "$CONFIG_FILE" "$INTEGRATION_TEST_DIR/"
#     cp "$META_CONFIG_FILE" "$INTEGRATION_TEST_DIR/"
#     echo "Copied config files to $INTEGRATION_TEST_DIR/"
# }

# if [ $# -eq 0 ]; then
#     echo "Usage: $0 <docker-compose-command>"
#     exit 1
# fi

# copy_config_file

# docker_cmd=$(build_docker_compose_command)

# $docker_cmd "$@"