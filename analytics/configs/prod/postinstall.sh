#!/bin/bash

set -e

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
if /etc/init.d/monit stop; then
	echo "Monit stopped unhappy"
fi

# Restart mongos (sometimes it consumes enough RAM to prevent analytics from loading)
restart mongos

# Stop and start the service
if status analytics-v1 | grep running; then
    stop analytics-v1
fi

sleep 30

if ! RESULT=`start analytics-v1 2>&1` > /dev/null ; then
        if echo "$RESULT" | grep -v "already running" > /dev/null ; then
		echo "Failure: $RESULT"
		exit 1
        fi
fi


# Restart monit to pick up changes
if /etc/init.d/monit start; then
	echo "Monit started unhappy";
fi

# Wait 20 seconds for startup, then test the health URL
sleep 20

echo "Checking health"
curl -v -f -G "http://localhost:30020/blueeyes/services/analytics/v1/health"
echo "Completed health check"

exit 0
