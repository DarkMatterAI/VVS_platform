#!/bin/bash

# Configuration file path
CONFIG_FILE="test_config.yaml"
INTEGRATION_EXECUTOR_DIR="./test_executor/src/"

run_unit_tests() {
    echo "Running unit tests..."
    grep -A2 "^  [^:]\+:" "$CONFIG_FILE" | awk '
    /^  [^:]+:/ {service=$1; sub(/:$/, "", service)}
    /service_name:/ {name=$2}
    /enabled:/ {enabled=$2}
    enabled=="True" && $0 ~ /^$/ {print name}
    ' | while read -r service; do
        echo "Running tests for $service"
        docker compose exec -T "$service" tests/run_tests.sh
    done
}

get_integration_test_info() {
    awk '/^integration_tests:/,/^$/' "$CONFIG_FILE" | grep -A3 "^  [^:]\+:" | awk '
    /^  [^:]+:/ {service=$1; sub(/:$/, "", service)}
    /service_name:/ {name=$2}
    /profile:/ {profile=$2}
    /enabled:/ {enabled=$2}
    enabled=="True" {print profile, name}
    ' | sort | uniq
}

get_enabled_integration_tests() {
    awk '
    /^(integration_tests|plugin_integration_tests):/ {
        in_section = 1
        next
    }
    /^[^ ]/ {
        in_section = 0
    }
    in_section && /enabled:/ {
        enabled = $2
        if (enabled == "True") {
            print "enabled"
        }
    }
    ' "$CONFIG_FILE" | grep -c "enabled"
}

any_integration_tests_enabled() {
    local count=$(get_enabled_integration_tests | wc -l)
    [ "$count" -gt 0 ]
}

run_integration_tests() {
    if ! any_integration_tests_enabled; then
        echo "No integration tests are enabled. Skipping integration tests."
        return
    fi

    echo "Starting integration test containers..."
    local cmd="docker compose"
    local services=""
    
    while read -r profile service; do
        cmd+=" --profile $profile"
        services+=" $service"
    done < <(get_integration_test_info)
    
    $cmd up -d
    
    echo "Copying test_config.yaml to ./test_executor/src/"
    cp "$CONFIG_FILE" "$INTEGRATION_EXECUTOR_DIR"
    
    echo "Starting integration test container..."

    docker compose --profile integration_test up -d --build --no-deps test_executor
    docker compose logs -f test_executor
    
    echo "Stopping and removing temporary containers..."
    services+=" test_executor"
    docker compose rm -sf $services
}

echo "Starting test suite..."

run_unit_tests

run_integration_tests

echo "Test suite completed."

