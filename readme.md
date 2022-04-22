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

### Load data

```
docker exec -it tc_app php scripts/load_clickhouse.php --table=movies
docker exec -it tc_app php scripts/load_clickhouse.php --table=movie_genres
docker exec -it tc_app php scripts/load_clickhouse.php --table=ratings
docker exec -it tc_app php scripts/load_clickhouse.php --table=tags
```

To access ClickHouse run `docker exec -it -uroot tc_clickhouse /usr/bin/clickhouse --client --database movies`

## MariaDB ColumnStore

To access MariaDB CLI run `docker exec -it tc_mariadb mariadb -D movies`

### Load data

```
docker exec -it tc_app php scripts/load_mariadb.php --table=movies
docker exec -it tc_app php scripts/load_mariadb.php --table=movie_genres
docker exec -it tc_app php scripts/load_mariadb.php --table=ratings
docker exec -it tc_app php scripts/load_mariadb.php --table=tags
```

## Run Top 100 Movies Queries

Get the top 100 movies with at least 100 ratings.

ClickHouse (Seasclick extension)
```
docker exec -it tc_app php reports/top-100-movies-seasclick.php
```


MariaDB ColumnStore (PDO MySQL)
```
docker exec -it tc_app php reports/top-100-movies-mariadb.php
```


#### PHPStorm Note

If you're using PHPStorm and want it recognize SeasClick class and it's methods please use this repository [patoui's PHPStorm Stubs](https://github.com/patoui/phpstorm-stubs/) and update your editors stubs to it's local path

PHPStorm via Settings -> Languages & Frameworks -> PHP -> PHP Runtime -> Advanced -> "Default stubs path"
