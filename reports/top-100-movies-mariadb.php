<?php

declare(strict_types=1);

require_once dirname(__DIR__) . '/vendor/autoload.php';

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

$time_start = microtime(true);

$sub_sql = <<<SQL
SELECT
    movies.movieId,
    COUNT(ratings.movieId) ratings_count,
    ROUND(AVG(ratings.rating), 2) ratings_avg
FROM movies.movies
JOIN movies.ratings ON ratings.movieId = movies.movieId
GROUP BY movies.movieId
HAVING ratings_count >= 100
ORDER BY ratings_avg DESC
LIMIT 100
SQL;

$sql = <<<SQL
SELECT
    movies.title,
    ratings_sub.ratings_count,
    ratings_sub.ratings_avg
FROM movies.movies
JOIN ($sub_sql) as ratings_sub ON movies.movieId = ratings_sub.movieId
ORDER BY ratings_avg DESC
LIMIT 100
SQL;

$movies = $client->query($sql);

$file_data = '';

foreach ($movies as $movie) {
    $file_data .= $movie_output = <<<"OUTPUT"
----------------------------------------------------------------------------
TITLE: {$movie['title']}
RATINGS COUNT: {$movie['ratings_count']}
RATINGS AVG: {$movie['ratings_avg']}\n
OUTPUT;
    echo $movie_output;
}

$time      = microtime(true) - $time_start;
$file_data .= $end_output = <<<"ENDOUTPUT"
----------------------------------------------------------------------------
Execution time: {$time} seconds\n
ENDOUTPUT;
echo $end_output;

file_put_contents(__DIR__ . '/top_100_mariadb_output.txt', $file_data);
