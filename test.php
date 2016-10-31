<?php

use Tajawal\MongoOdm\Config;
use Tajawal\MongoOdm\Database;

require 'vendor/autoload.php';

$config = new Config();
$config = $config->setHost('192.168.99.100')
                 ->setPort('37017')
                 ->setDatabase('tajawal_www_api')
                 ->setUsername(null)
                 ->setPassword(null);

$db    = Database::instance('default', $config);
$users = (new \Tajawal\MongoOdm\Collection('www_users'))->limit(10)->as_array();
$users = (new \Tajawal\MongoOdm\Collection('www_users'))->sort_desc('created_at')->limit(10)->as_array();
