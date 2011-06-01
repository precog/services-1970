java -jar ../analytics/analytics-v0.jar --configFile ../analytics/analytics.conf &
java -jar ../analytics/analytics-v0.jar --configFile benchmark_reports.conf & 
sleep 20
java -jar benchmark-v0.jar -cp=lib/commons-logging-1.0.4.jar --configFile benchmark.conf
