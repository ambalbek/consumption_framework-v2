# #!/bin/bash

# # Read from config.yml to decide whether to run consume-billing
# if grep -q 'billing_api_key:' /config.yml; then
#     # ESS environment
#     python main.py consume-billing
# fi
# python main.py consume-monitoring


#!/bin/bash
#LOG_FILE="/var/log/my_cron_job.log"
#Change to root directory 
cd /

# Read from config.yml to decide whether to run consume-billing
LOG_FILE="/var/log/my_cron_job.log"
if grep -q 'billing_api_key:' /config.yml; then
    # ESS environment
    echo "Consuming Billing"

    billing_api_key=$(grep 'billing_api_key:' /config.yml | sed -e 's/#.*//' -e 's/billing_api_key: *//' -e 's/^ *//' -e 's/ *$//')
    if [ -n "$billing_api_key" ]; then
        /usr/local/bin/python /main.py get-billing-data --lookbehind=24 >> "$LOG_FILE"
    fi
fi
echo "Consuming Monitoring"
/usr/local/bin/python /main.py consume-monitoring --lookbehind=24 >> "$LOG_FILE"