#!/bin/bash

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
/etc/init.d/monit stop

# Stop and start the service
stop jessup-v1
sleep 5

if ! RESULT=`start jessup-v1 2>&1` > /dev/null ; then
        if echo "$RESULT" | grep -v "already running" > /dev/null ; then
		exit 1
        fi
fi


# Restart monit to pick up changes
/etc/init.d/monit start

# Wait 30 seconds for startup, then test the health URL
sleep 30

set -e

curl -f -G "http://localhost:30030/blueeyes/services/jessup/v1/health"
