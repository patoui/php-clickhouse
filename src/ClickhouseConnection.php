<?php

declare(strict_types=1);

namespace App;

use Illuminate\Database\Connection;
use InvalidArgumentException;
use PDO;

class ClickhouseConnection extends Connection
{
    public function __construct(array $config = [])
    {
        $database = $config['database'] ?? null;
        if (!$database) {
            throw new InvalidArgumentException("'database' key is required");
        }

        $prefix = $config['prefix'] ?? '';
        unset($config['prefix']);

        $pdo = self::createPdoFromConfig($config);

        parent::__construct($pdo, $database, $prefix, $config);
    }

    private static function createPdoFromConfig(array $config): PDO
    {
        $driver   = 'mysql';
        $user     = $config['username'] ?? null;
        $password = $config['password'] ?? null;
        $options  = $config['options'] ?? null;
        unset($config['driver'], $config['username'], $config['password'], $config['options']);

        $config['dbname'] = $config['database'];
        unset($config['database']);

        $data_source_name = array_map(
            static function ($key, $value) {
                return "{$key}={$value}";
            },
            array_keys($config),
            array_values($config)
        );

        $data_source_name = "{$driver}:" . implode(';', $data_source_name);

        return new PDO($data_source_name, $user, $password, $options);
    }
}