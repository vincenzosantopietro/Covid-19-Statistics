# Covid Statistics 

This simple application reads COVID-19 data shared by italian Consiglio dei Ministri. This application is developed in
Scala, with Spark, stores data on InfluxDB and visualizes data trough Grafana Dashboards.

Probably scala is not the best tool for this kind of applications, but i decided to stick with it to practice with the
progamming language I use at work. 

## Dependencies

- InfluxDB
- Grafana
- Maven
- Spark

Both Grafana and InfluxDB are available in the container that docker will pull if you run the `start_docker.sh` script.

```shell script
docker pull philhawthorne/docker-influxdb-grafana:latest

docker run -d \
  --name docker-influxdb-grafana \
  -p 3003:3003 \
  -p 3004:8083 \
  -p 8086:8086 \
  -v influxdb:/var/lib/influxdb \
  -v grafana:/var/lib/grafana \
  philhawthorne/docker-influxdb-grafana:latest
```
You could also install both influx and grafana with `apt`. You decide.

## Run the Metrics Generator Job

You could either run the job using a spark-submit or import the project in Intellij and run the metrics computation by simply clicking the green play button.

```shell script
cd Covd-19-Statistics

mvn clean install assembly:single

spark-submit --class com.vinx.covid.statistics.job.NcovidStatisticsGeneratorJob \
 target/Covd-19-Statistics-1.0-SNAPSHOT-jar-with-dependencies.jar
```

![Image of Grafana Panels](src/main/resources/images/screenshot.png)

#### Additional Notes

A running demo can be found here: http://18.156.4.186:3000/d/-I6bRG9Zk/covid-19-statistics-italy?orgId=1&refresh=2h

Feel free to provide any feedback, suggest possible improvements or new data sources to "play" with. 

Stay safe and healthy.