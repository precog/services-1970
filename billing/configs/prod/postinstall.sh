#!/bin/bash

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
/etc/init.d/monit stop

# Stop and start the service
stop billing-v1
sleep 5

if ! RESULT=`start billing-v1 2>&1` > /dev/null ; then
        if echo "$RESULT" | grep -v "already running" > /dev/null ; then
		exit 1
        fi
fi


# Restart monit to pick up changes
/etc/init.d/monit start

# Wait 30 seconds for startup, then test the health URL
sleep 30

set -e

curl -f -G "http://localhost:30040/blueeyes/services/billing/v1/health"
