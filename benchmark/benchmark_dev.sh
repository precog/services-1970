#java -jar ../analytics/target/analytics-v0.jar --configFile ../analytics/analytics.conf &
java -jar ../analytics/target/analytics-v0.jar --configFile benchmark_reports.conf & 
sleep 20
java -jar ./target/benchmark-v0.jar --configFile benchmark_dev.conf
