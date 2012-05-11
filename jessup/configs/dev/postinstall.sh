#!/bin/bash

set -e

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
if /etc/init.d/monit stop; then
	echo "Monit stopped unhappy"
fi

# Stop and start the service
if status jessup-v1 | grep running; then
    stop jessup-v1
fi

sleep 5

if ! RESULT=`start jessup-v1 2>&1` > /dev/null ; then
	echo "Jessup didn't start: $RESULT"
        if echo "$RESULT" | grep -v "already running" > /dev/null ; then
		echo "Failure: $RESULT"
		exit 1
        fi
fi

echo "Jessup started, starting mongo"


# Restart monit to pick up changes
if /etc/init.d/monit start; then
	echo "Monit started unhappy";
fi

# Wait 30 seconds for startup, then test the health URL
sleep 30

echo "Checking health"
curl -f -G "http://localhost:30030/blueeyes/services/jessup/v1/health"
echo "Complete health check"

exit 0
