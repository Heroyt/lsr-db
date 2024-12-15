<?php

define('ROOT', dirname(__DIR__) . '/');
const TMP_DIR = ROOT . 'tests/tmp/';
const LOG_DIR = ROOT . 'tests/logs/';

ini_set('open_basedir', ROOT);

if (!file_exists(TMP_DIR) && !mkdir(TMP_DIR) && !is_dir(TMP_DIR)) {
    throw new Exception('Cannot create temporary directory: ' . TMP_DIR);
}

require_once ROOT . 'vendor/autoload.php';
