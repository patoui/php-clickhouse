<?php

declare(strict_types=1);

if (!function_exists('logger')) {
    function logger(string $message): void {
        file_put_contents(
            __DIR__ . '/logs/execution_results.txt',
            $message,
            FILE_APPEND
        );
    }
}
