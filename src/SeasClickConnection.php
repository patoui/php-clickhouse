<?php

declare(strict_types=1);

namespace App;

use Exception;
use SeasClick;

class SeasClickConnection
{
    /** @var SeasClick */
    private $client;

    public function __construct(array $config)
    {
        try {
            $this->client = new SeasClick($config);
            $this->client->select('SELECT 1');
        } catch (Exception $ex) {
            die('CLIENT EXCEPTION: ' . $ex->getMessage());
        }
    }

    public function getClient(): SeasClick
    {
        return $this->client;
    }
}