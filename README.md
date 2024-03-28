# hop-metrics

`hop-metrics` is a script designed to automatically gather daily metrics from SCiMMA's Kafka server, focusing on the influx of new messages each day. This tool is essential for administrators and users who need to monitor message traffic and ensure the Kafka server is performing optimally.

## Description

The `hop-metrics` script is scheduled to run once per day and collects information about new messages that have been published to SCiMMA's Kafka server. This automated process helps in maintaining a continuous overview of the server's activity, without manual intervention, making it an invaluable tool for system monitoring and analysis. All information is stored in an InfluxDB, where it's used to create usage reports characterizing the most active topics and groups.

## Usage

`hop-metrics` is set up to run via a cron job, executing automatically once every day (`0 0 * * *`). While the script operates autonomously, users looking to customize its schedule or behavior can do so by modifying the cron job settings.

## License

This project is licensed under the BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.

