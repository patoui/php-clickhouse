<?php

declare(strict_types=1);

use App\SeasClickConnection;

$connection = new SeasClickConnection([
    'host'        => 'tc_clickhouse',
    'port'        => 9000,
    'compression' => true,
]);
$client = $connection->getClient();

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

$movies = $client->select($sql);

echo "<table border='1' cellpadding='5'><thead><tr>";
echo "<th>Title</th><th># Ratings</th><th>Avg. Rating</th>";
echo "</tr></thead><tbody>";
foreach ($movies as $movie) {
    echo "<tr><td>{$movie['title']}</td><td>{$movie['ratings_count']}</td><td>{$movie['ratings_avg']}</td>";
}
echo "</tbody></table>";
