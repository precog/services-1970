java -jar ${RG_HOME}/services/analytics/target/analytics-v0.jar --configFile ${RG_HOME}/services/analytics/analytics.conf &
java -jar ${RG_HOME}/services/analytics/target/analytics-v1.jar --configFile ${RG_HOME}/services/analytics/benchmark_reports.conf 2>&1 > reports.log &
