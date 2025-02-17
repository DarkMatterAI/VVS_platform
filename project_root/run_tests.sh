#!/bin/bash

CONFIG_FILE="test_config.yaml"
META_CONFIG_FILE="test_config_meta.yaml"
INTEGRATION_EXECUTOR_DIR="./test_executor/src/"

# Read unit test services
unit_test_services=($(yq e '.services[] | select(.unit_test == True) | path | .[1]' "$CONFIG_FILE"))
echo "unit tests: ${unit_test_services[@]}"

# Check if any service has integration_test == true
integration_test_present=($(yq e '.services[] | select(.integration_test == True) | path | .[1]' "$CONFIG_FILE"))
echo "integration tests: ${integration_test_present[@]}"

# Read temp services to start
profiles=()  # Initialize empty array
temp_services_to_start=($(yq e '.temp_services[] | select(.enabled == True) | path | .[1]' "$CONFIG_FILE"))
for service in "${temp_services_to_start[@]}"; do
    profile=$(yq e ".temp_services.$service.profile" "$META_CONFIG_FILE")
    if [ ! -z "$profile" ]; then
        profiles+=("$profile")
    fi
done
echo "temp services: ${profiles[*]}"  # Use [*] for better output formatting

# Run unit tests
for service in "${unit_test_services[@]}"; do
    echo "Running unit tests for $service"
    docker compose exec -T "$service" tests/run_tests.sh
done

# Run integration tests if applicable
if [ ${#integration_test_present[@]} -gt 0 ] || [ ${#profiles[@]} -gt 0 ]; then
    if [ ${#profiles[@]} -gt 0 ]; then
        echo "Starting temp services"
        docker compose up -d "${profiles[@]}"
    fi

    echo "Copying config files to ./test_executor/src/"
    cp "$CONFIG_FILE" "$INTEGRATION_EXECUTOR_DIR"
    cp "$META_CONFIG_FILE" "$INTEGRATION_EXECUTOR_DIR"
    
    echo "Starting integration test container..."

    docker compose --profile integration_test up -d --build --no-deps test_executor
    docker compose logs -f test_executor

    # Remove test_executor and temp services
    echo "Removing test_executor and temp services"
    if [ ${#profiles[@]} -gt 0 ]; then
        docker compose rm -sf test_executor "${profiles[@]}"
    else
        docker compose rm -sf test_executor
    fi

else
    echo "No integration tests to run"
fi
