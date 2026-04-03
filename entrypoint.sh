#!/bin/bash
set -e
set -x

# If the command isn't "init", "get-billing-data", or "consume-monitoring", we assume the user wants to run a custom command.
if [ "$1" != "init" ] && [ "$1" != "get-billing-data" ] && [ "$1" != "consume-monitoring" ] && [ "$1" != "--help" ]; then
  exec "$@"
  exit $?
fi

# The first argument is the command to execute
# (e.g. "init", "get-billing-data", "consume-monitoring")
COMMAND=$1

# Everything after is considered additional parameters
ADDITIONAL_PARAMS=( "${@:2}" )

# Get all env variables starting with CF_ - these will be transparently passed to the given command
# We perform the following modifications:
# 1. We remove the CF_ prefix and replace it with -- (e.g. CF_API_KEY becomes --API_KEY)
# 2. We replace the = with a space (e.g. API_KEY=123 becomes --API_KEY 123)
# 3. We change the case of the key to lowercase (e.g. --API becomes --api)
# 4. We replace _ with - (e.g. API_KEY becomes --api-key)
# 5. In case the value has commas, multiple values will be passed (e.g. --API_KEY 123,456 becomes --api-key 123 --api-key 456)
# 6. We create a single line with all the key/value pairs

# We walk through the vars and create the final string
CF_ENV_VARS=""
for VAR in $(env | grep ^CF_); do
  # Split the key and value
  KEY=$(echo $VAR | cut -d= -f1)
  VALUE=$(echo $VAR | cut -d= -f2-)  # We use -f2- to get everything after the first = sign

  # Step 1: Remove CF_ prefix and replace it with --
  KEY=$(echo $KEY | sed 's/^CF_/--/')

  # Step 2 is already done by the split

  # Step 3: Change the case of the key to lowercase
  KEY=$(echo $KEY | tr '[:upper:]' '[:lower:]')

  # Step 4: Replace _ with -
  KEY=$(echo $KEY | sed 's/_/-/g')

  # Step 5: In case the value has commas, split it
  if (echo $VALUE | grep -q ","); then
    for SPLIT_VALUE in $(echo $VALUE | tr "," "\n"); do
      CF_ENV_VARS="$CF_ENV_VARS $KEY $SPLIT_VALUE"
    done
  else
    CF_ENV_VARS="$CF_ENV_VARS $KEY $VALUE"
  fi
done

# CFCONF_ env variables are passed as --config parameters
for VAR in $(env | grep ^CFCONF_); do
  # Replace CFCONF_ prefix with --config
  CONF=$(echo $VAR | sed 's/^CFCONF_/--config /' | tr '[:upper:]' '[:lower:]' | sed 's/__/./')
  CF_ENV_VARS="$CF_ENV_VARS $CONF"
done

# Final step: Create a single line with all the key/value pairs
CF_ENV_VARS=$(echo $CF_ENV_VARS | tr " " "\n")

# Execute the command, and pass along additional parameters:
# 1. Additional command params
# 2. CF_ env variables
exec python3 /app/main.py "$COMMAND" "${ADDITIONAL_PARAMS[@]}" $CF_ENV_VARS
