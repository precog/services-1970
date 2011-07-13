ps auxww | grep java | grep analytics-v0.jar | awk '{print $2}' | xargs kill -9
ps auxww | grep java | grep benchmark-v0.jar | awk '{print $2}' | xargs kill -9
