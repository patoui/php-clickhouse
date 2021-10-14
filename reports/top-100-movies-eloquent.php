<?php

declare(strict_types=1);

use Illuminate\Database\Capsule\Manager as Connection;

$connection = Connection::connection();

$ratings_sub = $connection->table('movies')
    ->select([
        'movies.movieId',
        $connection->raw('count(ratings.movieId) ratings_count'),
        $connection->raw('round(avg(ratings.rating), 2) ratings_avg')
    ])
    ->join('ratings', 'ratings.movieId', '=', 'movies.movieId')
    ->groupBy('movies.movieId')
    ->having('ratings_count', '>=', 100)
    ->orderByDesc('ratings_avg')
    ->limit(100);

$movies = $connection->table('movies')
    ->select(['movies.title', 'ratings_sub.ratings_count', 'ratings_sub.ratings_avg'])
    ->joinSub($ratings_sub, 'ratings_sub', 'movies.movieId', '=', 'ratings_sub.movieId')
    ->orderByDesc('ratings_avg')
    ->limit(100)
    ->get();

echo "<table border='1' cellpadding='5'><thead><tr>";
echo "<th>Title</th><th># Ratings</th><th>Avg. Rating</th>";
echo "</tr></thead><tbody>";
foreach ($movies as $movie) {
    echo "<tr><td>{$movie->title}</td><td>{$movie->ratings_count}</td><td>{$movie->ratings_avg}</td>";
}
echo "</tbody></table>";
