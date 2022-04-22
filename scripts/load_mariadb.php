<?php

declare(strict_types=1);

require_once dirname(__DIR__) . '/vendor/autoload.php';

ignore_user_abort(true);
set_time_limit(0);

$opts  = getopt('t:', ['table:']);
$table = $opts['t'] ?? $opts['table'] ?? null;

if (!$table) {
    throw new \InvalidArgumentException('Must pass in table via -t or --table');
}

$user     = 'admin';
$password = 'strong_Password123';

$client = new PDO('mysql:host=mariadb;port=3306;charset=utf8mb4', $user, $password, [
    // Turn off persistent connections
    PDO::ATTR_PERSISTENT         => false,
    // Enable exceptions
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    // Emulate prepared statements
    PDO::ATTR_EMULATE_PREPARES   => true,
    // Set default fetch mode to array
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    // Set character set
    PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci',
]);

$client->exec('CREATE DATABASE IF NOT EXISTS movies');
$client->exec('USE movies');

if ($table === 'movies') {
    $client->exec(<<<SQL
    CREATE TABLE IF NOT EXISTS movies (
        `timestamp` TIMESTAMP,
        movieId     INT UNSIGNED,
        title       VARCHAR(255),
        year        SMALLINT UNSIGNED
    ) ENGINE=columnstore;
    SQL
    );
    $client->exec('TRUNCATE TABLE movies;');
}

if ($table === 'movie_genres') {
    $client->exec(<<<SQL
    CREATE TABLE IF NOT EXISTS movie_genres (
        `timestamp` TIMESTAMP,
        movieId     INT UNSIGNED,
        genre       VARCHAR(255)
    ) ENGINE=columnstore;
    SQL
    );
    $client->exec('TRUNCATE TABLE movie_genres;');
}

if ($table === 'ratings') {
    $client->exec(<<<SQL
    CREATE TABLE IF NOT EXISTS ratings (
        userId      INT UNSIGNED,
        movieId     INT UNSIGNED,
        rating      FLOAT,
        `timestamp` TIMESTAMP
    ) ENGINE=columnstore
    SQL
    );
    $client->exec('TRUNCATE TABLE ratings;');
}

if ($table === 'tags') {
    $client->exec(<<<SQL
    CREATE TABLE IF NOT EXISTS tags (
        userId      INT UNSIGNED,
        movieId     INT UNSIGNED,
        tag         VARCHAR(255),
        `timestamp` TIMESTAMP
    ) ENGINE=columnstore
    SQL
    );
    $client->exec('TRUNCATE TABLE tags;');
}

$get_table_count = static function ($table) use ($client): int {
    try {
        return (int) $client->query("SELECT COUNT(*) FROM {$table}")->fetchColumn();
    } catch (Exception $ex) {
        return 0;
    }
};

$time_start = microtime(true);

$insert_csv = static function (
    string $csv,
    string $table
) use ($client, $get_table_count) {
    $fp                   = null;
    $is_header            = true;
    $columns              = '';
    $fp                   = $fp ?: fopen($csv, 'rb');
    $placeholder_template = '';
    $placeholders         = '';
    $bindings             = [];
    $created_at           = date('Y-m-d H:i:s');
    $row_count            = 0;
    $has_timestamp        = false;

    if ($fp === false) {
        return;
    }

    $get_columns = static function ($columns, $has_timestamp) {
        if (!$has_timestamp) {
            $columns[] = 'timestamp';
        }
        $col_str = '';
        foreach ($columns as $column) {
            $col_str .= $column . ',';
        }
        return rtrim($col_str, ',');
    };

    $perform_insert = static function ($columns, $placeholders, $bindings) use ($client, $table) {
        try {
            $stmt = $client->prepare(
                "INSERT INTO {$table} ({$columns}) VALUES " . rtrim($placeholders, ',')
            );
            $stmt->execute(array_merge([], ...$bindings));
        } catch (Throwable $throwable) {
//            var_dump($bindings);
            throw $throwable;
        }
    };

    while (($row = fgetcsv($fp)) !== false) {
        if ($is_header) {
            $has_timestamp        = in_array('timestamp', $row, true);
            $columns              = $get_columns($row, $has_timestamp);
            $is_header            = false;
            $placeholder_template = '(' . rtrim(str_repeat('?,', count($row) + ($has_timestamp ? 0 : 1)), ',') . '),';
            continue;
        }

        // empty year field
        if ($table === 'movies' && empty($row[2])) {
            continue;
        }

        if($table === 'ratings' || $table === 'tags') {
            $row[3] = date('Y-m-d H:i:s', (int) $row[3]);
        }

        if (!$has_timestamp) {
            $row[] = $created_at;
        }
        $placeholders .= $placeholder_template;
        $bindings[]   = $row;
        ++$row_count;

        if ($row_count >= 50000) {
            $perform_insert($columns, $placeholders, $bindings);
            $placeholders = '';
            $bindings     = [];
            $row_count    = 0;
        }
    }

    if ($row_count) {
        $perform_insert($columns, $placeholders, $bindings);
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