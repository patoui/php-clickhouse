# PHP and ClickHouse Docker Setup

Using this as a sandbox to learn about how PHP and ClickHouse perform queries on large datasets.

This repo will use SeasX/SeasClick C++ extension to run ClickHouse queries.

## Installation

```bash
docker-compose up
```

## Web

Visit `localhost` or `127.0.0.1`

## ClickHouse

To access ClickHouse run `docker exec -it -uroot tc_clickhouse /usr/bin/clickhouse --client`

#### PHPStorm Note

If you're using PHPStorm and want it recognize SeasClick class and it's methods please use this repository [patoui's PHPStorm Stubs](https://github.com/patoui/phpstorm-stubs/) and update your editors stubs to it's local path

PHPStorm via Settings -> Languages & Frameworks -> PHP -> PHP Runtime -> Advanced -> "Default stubs path"
