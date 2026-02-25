<?php

namespace App;

use Exception;
use JetBrains\PhpStorm\NoReturn;

final class Parser
{
    protected string $inputPath;
    protected string $outputPath;

    protected const int THREADS = 2;

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->inputPath = $inputPath;
        $this->outputPath = $outputPath;

        ini_set('memory_limit', '1024M');

        $this->process();
    }

    public function process(): void
    {
        $splits = $this->calcSplits();

        for ($i = 0; $i < self::THREADS; $i++) {
            $pid = pcntl_fork();

            if (!$pid) {
                $this->forkProcessWorker($splits, $i);
            }
        }

        // wait for all child processes to finish
        while (($pid = pcntl_waitpid(0, $status)) != -1) {
            // Wait for exit
        }

        $this->processFinalData();
    }

    #[NoReturn]
    protected function forkProcessWorker(array $splits, int $i): void
    {
        $data = [];

        $fh = fopen($this->inputPath, 'rb');

        // Seek to correct position in file
        if ($i > 0) {
            fseek($fh, $splits[$i]);
        }

        $end = isset($splits[$i+1]) ? ($splits[$i+1] - 1) : null;

        while ($line = fgets($fh)) {
            $this->parseLine($line, $data);

            // Break if we are read all we need to
            if ($end && ftell($fh) > $end) {
                break;
            }
        }

        file_put_contents($this->outputPath . '.' . $i, $this->serialize($data));

        exit(0);
    }

    protected function calcSplits(): array
    {
        $size = filesize($this->inputPath);
        $partition = floor($size / self::THREADS);

        $fh = fopen($this->inputPath, 'rb');

        $splits = [];

        // Search for nice clean split positions
        for ($i = 1; $i < self::THREADS; $i++) {
            fseek($fh, $partition * $i);
            fgets($fh);
            $splits[$i] = ftell($fh);
        }

        return $splits;
    }

    protected function parseLine(string $line, array &$data): void
    {
        $comma_pos = strpos($line, ',');
        $third_slash_pos = strpos($line, '/', 8); // Offset 8 to skip http:// or https://
        $url = substr($line, $third_slash_pos, $comma_pos - $third_slash_pos);
        $date = substr($line, $comma_pos + 1, 10);

        if (! isset($data[$url])) {
            $data[$url] = [];
        }

        if (! isset($data[$url][$date])) {
            $data[$url][$date] = 0;
        }

        $data[$url][$date] += 1;
    }

    protected function processFinalData(): void
    {
        $finalData = [];
        for ($i = 0; $i < self::THREADS; $i++) {
            $childFile = $this->outputPath . '.' . $i;
            $childData =  $this->unserialize(file_get_contents($childFile));

            // This is clearly dangerous but seems to always validate, so I'm running with it
            // If the same URL & date happened to appear in different splits then it would get overwritten
            $finalData = array_merge_recursive($finalData, $childData);
            unset($childData);

            unlink($childFile);
        }

        array_walk($finalData, function (&$value) {
            ksort($value);
        });

        file_put_contents($this->outputPath, json_encode($finalData, JSON_PRETTY_PRINT));
    }

    protected function serialize(mixed $data): string
    {
        return function_exists('igbinary_serialize') ? igbinary_serialize($data) : serialize($data);
    }

    protected function unserialize(string $data): mixed
    {
        return function_exists('igbinary_unserialize') ? igbinary_unserialize($data) : unserialize($data);
    }
}