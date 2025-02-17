#!/bin/bash

CONFIG_FILE="test_config.yaml"
INTEGRATION_EXECUTOR_DIR="./test_executor/src/"

# Read unit test services
unit_test_services=($(yq e '.services[] | select(.unit_test == True) | .service_name' "$CONFIG_FILE"))
echo unit tests $unit_test_services

# Check if any service has integration_test == true
integration_test_present=($(yq e '.services[] | select(.integration_test == True) | .service_name' "$CONFIG_FILE"))
echo integration tests $integration_test_present

# Read temp services to start
temp_services_to_start=($(yq e '.temp_services[] | select(.enabled == True) | .profile' "$CONFIG_FILE"))
echo temp services $temp_services_to_start


# Run unit tests
for service in "${unit_test_services[@]}"; do
    echo "Running unit tests for $service"
    docker compose exec -T "$service" tests/run_tests.sh
done


# Run integration tests if applicable
if [ -n "$integration_test_present" ] || [ -n "$temp_services_to_start" ]; then
    if [ -n "$temp_services_to_start" ]; then
        echo "Starting temp services"
        docker compose up -d ${temp_services_to_start[@]}
    fi

    echo "Copying test_config.yaml to ./test_executor/src/"
    cp "$CONFIG_FILE" "$INTEGRATION_EXECUTOR_DIR"
    
    echo "Starting integration test container..."

    docker compose --profile integration_test up -d --build --no-deps test_executor
    docker compose logs -f test_executor

    # Remove test_executor and temp services
    echo "Removing test_executor and temp services"
    docker compose rm -sf test_executor ${temp_services_to_start[@]}

else
    echo "No integration tests to run"
fi

