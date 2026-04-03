#!/bin/bash
monitoring_retry="true"
monitoring_timeout="60"
monitoring_max_retries="5"
cron_schedule="0 6,18 * * *"



interpret_tf() {
    case $1 in
        [tT]|[tT][rR][uU][eE]) echo "true" ;;
        [fF]|[fF][aA][lL][sS][eE]) echo "false" ;;
        *) echo "Invalid input. Please enter 't' (true) or 'f' (false)." && exit 1 ;;
    esac
}


# Function to create config.yml line by line
create_config() {
    # Initialize config.yml
    > config.yml

    echo "Creating config.yml..."
    echo "---" >> config.yml
    # Example and user input for each field
    echo "Enter organization_id (e.g., '12345'):"
    read organization_id
    echo "organization_id: \"$organization_id\"" >> config.yml

    echo "Enter organization_name (e.g., 'My organization'):"
    read organization_name
    echo "organization_name: \"$organization_name\"" >> config.yml

    if [ "$1" == "ESS" ]; then
        echo "Enter billing_api_key (e.g., 'essu_XXXX'):"
        read billing_api_key
        echo "billing_api_key: \"$billing_api_key\"" >> config.yml
    fi

    echo "Enter monitoring_source hosts (e.g., 'https://where-i-want-to-get-the-data:443'):"
    read monitoring_hosts
    echo "monitoring_source:" >> config.yml
    echo "  hosts: '$monitoring_hosts'" >> config.yml

    echo "Enter monitoring_source api_key:"
    read monitoring_api_key
    echo "  api_key: '$monitoring_api_key'" >> config.yml
    echo "  max_retries: $monitoring_max_retries" >> config.yml
    echo "  retry_on_timeout: $monitoring_retry" >> config.yml
    echo "  request_timeout: $monitoring_timeout" >> config.yml

    # Verify certs prompt
    read -p "Should monitoring_source verify_certs be enabled? (true/false, default: true): " input_monitoring_verify_certs
    input_monitoring_verify_certs=${input_monitoring_verify_certs,,}  # Convert input to lowercase
    monitoring_verify_certs=${input_monitoring_verify_certs:-true}
    if [ "$monitoring_verify_certs" != "true" ] && [ "$monitoring_verify_certs" != "false" ]; then
        echo "Invalid input. Please enter 'true' or 'false'." && exit 1
    fi
    echo "  verify_certs: $monitoring_verify_certs" >> config.yml

    echo "Enter consumption_destination hosts (e.g., 'https://where-i-want-the-data-to-be-indexed:443'):"
    read consumption_hosts
    echo "consumption_destination:" >> config.yml
    echo "  hosts: '$consumption_hosts'" >> config.yml

    echo "Enter consumption_destination api_key:"
    read consumption_api_key
    echo "  api_key: '$consumption_api_key'" >> config.yml

    echo "  max_retries: $monitoring_max_retries" >> config.yml
    echo "  retry_on_timeout: $monitoring_retry" >> config.yml
    echo "  request_timeout: $monitoring_timeout" >> config.yml
    # Verify certs prompt
    read -p "Should consumption_source verify_certs be enabled? (true/false, default: true): " input_consumption_verify_certs
    input_consumption_verify_certs=${input_consumption_verify_certs,,}  # Convert input to lowercase
    consumption_verify_certs=${input_consumption_verify_certs:-true}
    if [ "$consumption_verify_certs" != "true" ] && [ "$consumption_verify_certs" != "false" ]; then
        echo "Invalid input. Please enter 'true' or 'false'." && exit 1
    fi
    echo "  verify_certs: $consumption_verify_certs" >> config.yml

    if [ "$1" == "On-prem" ]; then
        echo "Enter on_prem_costs for hot (e.g., 1.0):"
        read hot_cost
        echo "on_prem_costs:" >> config.yml
        echo "  hot: $hot_cost" >> config.yml

        echo "Enter on_prem_costs for warm (e.g., 0.5):"
        read warm_cost
        echo "  warm: $warm_cost" >> config.yml

        echo "Enter on_prem_costs for cold (e.g., 0.25):"
        read cold_cost
        echo "  cold: $cold_cost" >> config.yml

        echo "Enter on_prem_costs for frozen (e.g., 0.1):"
        read frozen_cost
        echo "  frozen: $frozen_cost" >> config.yml
    fi


}

check_existing_config() {
    if [ -f "config.yml" ]; then
        echo "A config.yml file already exists in the current directory."
        read -p "Do you want to use the existing file? (yes/no): " use_existing

        if [ "$use_existing" == "yes" ]; then
            echo "Using the existing config.yml file."
            return 1
        fi
    fi
    return 0
}

# Menu for selecting environment type
echo "Select the environment type:"
echo "1) ESS (default)"
echo "2) On-prem"
read -p "Enter your choice (1/2): " environment_choice

if check_existing_config; then
    case $environment_choice in
        1|"")
            echo "Selected ESS environment."
            create_config "ESS"
            ;;
        2)
            echo "Selected On-prem environment."
            create_config "On-prem"
            ;;
        *)
            echo "Invalid choice."
            exit 1
            ;;
    esac
fi

if [ "$change_schedule" = "yes" ]; then
    echo "Enter the cron schedule for running the tasks (e.g., '0 1 * * *' for every day at 1 AM):"
    read new_cron_schedule
    if [ -n "$new_cron_schedule" ]; then
        cron_schedule=$new_cron_schedule
    fi
fi

###Not used: Docker Save: docker save elastic_consumption_framework:latest > elastic_consumption_framework_latest_$(uname -m).tar
##

echo "Loading Docker image..."

# Check if the Docker image tar file exists
image_tar="elastic_consumption_framework_latest_$(uname -m).tar"
if [ -f "$image_tar" ]; then
    echo "Loading Docker image from tar file..."
    docker load < "$image_tar"
else
    echo "Docker image tar file not found."
    read -p "Do you want to build the Docker image? [yes/no]: " build_choice

    if [ "$build_choice" = "yes" ]; then
        echo "Building Docker image..."
        docker build -t elastic_consumption_framework:latest .
    else
        echo "Please provide the Docker image tar file for an air-gapped environment."
        echo "Once the file is provided, run this script again."
        exit 1
    fi
fi

##    --security-opt seccomp=unconfined \ needed for running on ece
# Run the Docker container with the cron schedule environment variable
docker run -d --name consumption_framework \
    -e CRON_SCHEDULE="$cron_schedule" \
    -v "$(pwd)/config.yml:/config.yml" \
    --restart always \
    elastic_consumption_framework:latest


# Run the container with the appropriate arguments based on environment type
echo "Starting the application..."
# docker run command with 'consume-billing' and/or 'consume-monitoring'
