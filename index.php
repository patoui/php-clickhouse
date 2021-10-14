<?php

declare(strict_types=1);

use App\ClickhouseConnection;
use Illuminate\Database\Connection;

require_once 'vendor/autoload.php';

if (!extension_loaded('SeasClick')) {
    exit('Extension not loaded: SeasClick.' . PHP_SHLIB_SUFFIX);
}

try {
    $client = new SeasClick([
        'host'        => 'tc_clickhouse',
        'port'        => 9000,
        'compression' => true,
    ]);
    $client->select('SELECT 1');
} catch (Exception $ex) {
    die('CLIENT EXCEPTION: ' . $ex->getMessage());
}

die('Successfully connected');




// TODO: determine how to zip or chunk files to get under 100MB limit on GitHub
// TODO: determine how to make SeasClick work with PHP 7.4
// Issue only 'SeasClickException' is available on PHP 7.4 but SeasClick::AS_DATE_FORMAT works
//var_dump(array_filter(get_declared_classes(), static function ($v) {
//    return stripos($v, 'SeasClick') !== false;
//}));
//exit();
//Fatal error: Uncaught Error: Class 'SeasClick' not found in /var/www/src/SeasClickConnection.php:18 Stack trace: #0 /var/www/reports/top-100-movies-seasclick.php(8): App\SeasClickConnection->__construct(Array) #1 /var/www/index.php(64): require_once('/var/www/report...') #2 {main} thrown in /var/www/src/SeasClickConnection.php on line 18

/** @property SeasClick $client */
$app = new stdClass();

function setupConnection(stdClass $container, string $driver): void
{
    if ($driver !== 'seasclick' && $driver !== 'eloquent') {
        throw new \InvalidArgumentException('Accepted drivers are: seasclick, eloquent');
    }

    if ($driver === 'eloquent') {
        Connection::resolverFor('clickhouse', static function ($conn, $db, $prefix, $config) {
            return new ClickhouseConnection($config);
        });

        $capsule = new Illuminate\Database\Capsule\Manager();

        $capsule->addConnection([
            'driver'   => 'clickhouse',
            'host'     => 'tc_clickhouse',
            'database' => 'movies',
            'port'     => 9004,
            'username' => 'default',
            'password' => '',
            'options'  => [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        ]);
        $capsule->setAsGlobal();
        $capsule->bootEloquent();
    }
}

$driver = $_GET['driver'] ?? 'seasclick';
setupConnection($app, $driver);

$time_start = microtime(true);

$report = $_GET['report'] ?? null;
$db     = $_GET['db'] ?? null;

if ($report) {
    $report    .= "-{$driver}";
    $file_path = __DIR__ . "/reports/{$report}.php";
    if (file_exists($file_path)) {
        require_once $file_path;
    }
} elseif ($db) {
    if ($db === 'movies') {
        $app->client->execute('CREATE DATABASE IF NOT EXISTS movies');

        $file_path = __DIR__ . "/databases/{$db}/loader.php";

        if (file_exists($file_path)) {
            require_once $file_path;
        }
    }
}

$time_end = microtime(true);
$time     = $time_end - $time_start;
echo "Execution time: {$time} seconds" . PHP_EOL;

die('Finished');
