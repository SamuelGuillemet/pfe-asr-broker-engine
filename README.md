# Market broker

## Description

This is a simple market broker that allows you to buy and sell stocks at market price.

## How to run

### Requirements

- Java 17
- Docker

### Running the application

Run the following command to init the kafka broker, the schema registry and the postgres database:

```bash
$ docker-compose up -d
```

You can use the following script to run the components:

```bash
$ ./start-app.sh
```

To fill up the market-data topics you will need this project [pfe-asr-broker-preprocessing](https://github.com/SamuelGuillemet/pfe-asr-broker-preprocessing).
