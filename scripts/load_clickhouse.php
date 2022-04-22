<?php

declare(strict_types=1);

use App\SeasClickConnection;

require_once dirname(__DIR__) . '/vendor/autoload.php';

ignore_user_abort(true);
set_time_limit(0);

if (!extension_loaded('SeasClick')) {
    exit('Extension not loaded: SeasClick.' . PHP_SHLIB_SUFFIX);
}

$opts  = getopt('t:', ['table:']);
$table = $opts['t'] ?? $opts['table'] ?? null;

if (!$table) {
    throw new \InvalidArgumentException('Must pass in table via -t or --table');
}

$connection = new SeasClickConnection([
    'host'        => 'tc_clickhouse',
    'port'        => 9000,
    'compression' => true,
]);

/** @property SeasClick $client */
$client = $connection->getClient();

$get_table_count = static function ($table) use ($client): int {
    try {
        return $client->select("SELECT COUNT() as count FROM {$table}")[0]['count'] ?? 0;
    } catch (Exception $ex) {
        return 0;
    }
};

$get_file_line_count = static function ($fp) {
    rewind($fp);
    $lines = 0;
    while (!feof($fp)) {
        $lines += substr_count(fread($fp, 8192), "\n");
    }
    return $lines;
};

$client->execute('CREATE DATABASE IF NOT EXISTS movies');
$client->execute('USE movies');

if ($table === 'movies') {
    $client->execute(<<<SQL
    CREATE TABLE IF NOT EXISTS movies (
        dt        Date DEFAULT toDate(createdAt),
        createdAt DateTime,
        movieId   UInt32,
        title     String,
        year      UInt16
    ) ENGINE = MergeTree (dt, (movieId, dt), 8192);
    SQL
    );
    $client->execute('TRUNCATE TABLE IF EXISTS movies');
}

if ($table === 'movie_genres') {
    $client->execute(<<<SQL
    CREATE TABLE IF NOT EXISTS movie_genres (
        dt        Date DEFAULT toDate(createdAt),
        createdAt DateTime,
        movieId   UInt32,
        genre     String
    ) ENGINE = MergeTree (dt, (genre, dt), 8192);
    SQL
    );
    $client->execute('TRUNCATE TABLE IF EXISTS movie_genres');
}

if ($table === 'ratings') {
    $client->execute(<<<SQL
    CREATE TABLE IF NOT EXISTS ratings (
        dt        Date DEFAULT toDate(timestamp),
        userId    UInt32,
        movieId   UInt32,
        rating    Float32,
        timestamp DateTime
    ) ENGINE = MergeTree (dt, (movieId, dt), 8192);
    SQL
    );
    $client->execute('TRUNCATE TABLE IF EXISTS ratings');
}

if ($table === 'tags') {
    $client->execute(<<<SQL
    CREATE TABLE IF NOT EXISTS tags (
        dt        Date DEFAULT toDate(timestamp),
        userId    UInt32,
        movieId   UInt32,
        tag       String,
        timestamp DateTime
    ) ENGINE = MergeTree (dt, (movieId, dt), 8192);
    SQL
    );
    $client->execute('TRUNCATE TABLE IF EXISTS tags');
}

$time_start = microtime(true);

$insert_csv = static function (
    string $csv,
    string $table
) use ($client, $get_table_count, $get_file_line_count) {
    $fp            = null;
    $is_header     = true;
    $rows          = [];
    $headers       = [];
    $fp            = $fp ?: fopen($csv, 'rb');
    $has_timestamp = false;

    if ($fp === false) {
        return;
    }

    $get_headers = static function (array $row, bool $has_timestamp) {
        $headers = $row;
        if (!$has_timestamp) {
            $headers[] = 'createdAt';
        }
        return $headers;
    };

    while (($row = fgetcsv($fp)) !== false) {
        if ($is_header) {
            $has_timestamp = in_array('timestamp', $row, true);
            $headers       = $get_headers($row, $has_timestamp);
            $is_header     = false;
            continue;
        }

        // empty year field
        if ($table === 'movies' && empty($row[2])) {
            continue;
        }

        if (!$has_timestamp) {
            $row[] = time();
        }
        $rows[] = $row;

        // Chunking, otherwise errors about partition size occurs
        if (count($rows) >= 100) {
            try {
                $client->insert($table, $headers, $rows);
            } catch (Exception $exception) {
                echo 'EXCEPTION WHILE INSERTING' . PHP_EOL;
            }
            $rows = [];
        }
    }

    if (!empty($rows)) {
        $client->insert($table, $headers, $rows);
    }

    $table_count = $get_table_count($table);

    $end_msg = "Record count for '{$table}': {$table_count}";
    echo $end_msg . PHP_EOL;
    file_put_contents(dirname(__DIR__) . '/logs/execution_results.txt', $end_msg, FILE_APPEND);
};

$insert_csv(dirname(__DIR__) . "/databases/movies/{$table}.csv", $table);

$time_end = microtime(true);
$time     = $time_end - $time_start;
$output   = "Execution time: {$time} seconds";
echo $output . PHP_EOL;
file_put_contents(dirname(__DIR__) . '/logs/execution_results.txt', $output, FILE_APPEND);
