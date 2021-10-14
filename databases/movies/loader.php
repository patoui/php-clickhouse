<?php

declare(strict_types=1);

use App\SeasClickConnection;

require_once dirname(__DIR__, 2) . '/vendor/autoload.php';

ignore_user_abort(true);
set_time_limit(0);

if (!extension_loaded('SeasClick')) {
    exit('Extension not loaded: SeasClick.' . PHP_SHLIB_SUFFIX);
}

$time_start = microtime(true);

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

$client->execute(<<<SQL
CREATE TABLE IF NOT EXISTS movies.movies (
    dt        Date DEFAULT toDate(createdAt),
    createdAt DateTime,
    movieId   UInt32,
    title     String,
    year      UInt16
) ENGINE = MergeTree (dt, (movieId, dt), 8192);
SQL
);

$client->execute(<<<SQL
CREATE TABLE IF NOT EXISTS movies.movie_genres (
    dt        Date DEFAULT toDate(createdAt),
    createdAt DateTime,
    movieId   UInt32,
    genre     String
) ENGINE = MergeTree (dt, (genre, dt), 8192);
SQL
);

$client->execute(<<<SQL
CREATE TABLE IF NOT EXISTS movies.ratings (
    dt        Date DEFAULT toDate(timestamp),
    userId    UInt32,
    movieId   UInt32,
    rating    Float32,
    timestamp DateTime
) ENGINE = MergeTree (dt, (movieId, dt), 8192);
SQL
);

$client->execute(<<<SQL
CREATE TABLE IF NOT EXISTS movies.tags (
    dt        Date DEFAULT toDate(timestamp),
    userId    UInt32,
    movieId   UInt32,
    tag       String,
    timestamp DateTime
) ENGINE = MergeTree (dt, (movieId, dt), 8192);
SQL
);

$insert_csv = static function (
    string $csv,
    string $table,
    bool $with_created_at = true,
    bool $check_if_exist = false
) use ($client, $get_table_count, $get_file_line_count) {
    // TODO: add flag to truncate/reset
//    if ($get_table_count($table) > 0) {
//        $client->execute('TRUNCATE TABLE IF EXISTS {table}', ['table' => $table]);
//    }

    $fp = null;
    if (($table_count = $get_table_count($table)) > 0) {
        $fp    = fopen($csv, 'rb');
        $lines = $get_file_line_count($fp);

        // line count minus header record count should be in the table
        if ($table_count >= ($lines - 1)) {
            return;
        }

        // reset file pointer for processing loop
        rewind($fp);
    }

    $is_header = true;
    $rows      = [];
    $headers   = [];
    $fp        = $fp ?: fopen($csv, 'rb');
    $last_file = __DIR__ . '/last_insert_' . str_replace('.', '_', $table) . '.txt';

    if ($fp === false) {
        return;
    }

    $table_desc = null;
    if ($check_if_exist) {
        $table_desc = array_reduce($client->select("DESC {$table}"), static function ($carry, $value) {
            $carry[$value['name']] = $value['type'];
            return $carry;
        }, []);
    }

    $get_headers = static function ($row) use ($with_created_at) {
        $headers = $row;
        if ($with_created_at) {
            $headers[] = 'createdAt';
        }
        return $headers;
    };

    if (file_exists($last_file)) {
        $last_pointer = file_get_contents($last_file);
        if ($last_pointer) {
            $headers = $get_headers(fgetcsv($fp));
            fseek($fp, (int) $last_pointer);
            $is_header = false;
        }
    }

    while (($row = fgetcsv($fp)) !== false) {
        if ($is_header) {
            $headers   = $get_headers($row);
            $is_header = false;
            continue;
        }

        if ($check_if_exist) {
            $sql = "SELECT 1 FROM {$table} WHERE ";

            foreach ($row as $key => $value) {
                $column_name = $headers[$key];
                $type        = $table_desc[$column_name] ?? '';
                $value       = stripos($type, 'String') !== false ? "'{$value}'" : $value;
                $sql         .= "{$column_name} = {$value} AND ";
            }
            $sql = trim($sql, 'AND ');

            // skip if exist already
            if ($client->select($sql)) {
                continue;
            }
        }

        if ($with_created_at) {
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
            file_put_contents($last_file, ftell($fp));
        }
    }

    if (!empty($rows)) {
        $client->insert($table, $headers, $rows);
    }

    $table_count = $get_table_count($table);
    $line_count  = $get_file_line_count($fp);

    if ($table_count >= ($line_count - 1)) {
        unlink($last_file);
    }

    echo "Record count for '{$table}': {$table_count}" . PHP_EOL;
    flush();
    ob_flush();
};

//$insert_csv(__DIR__ . '/movies.csv', 'movies.movies');
//$insert_csv(__DIR__ . '/movie_genres.csv', 'movies.movie_genres');
//$insert_csv(__DIR__ . '/ratings.csv', 'movies.ratings', false);
//$insert_csv(__DIR__ . '/tags.csv', 'movies.tags', false);

$time_end = microtime(true);
$time     = $time_end - $time_start;
$output   = "Execution time: {$time} seconds";
echo $output . PHP_EOL;
file_put_contents(__DIR__ . '/execution_results.txt', $output);
