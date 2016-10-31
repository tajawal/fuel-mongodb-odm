<?php

namespace Tajawal\MongoOdm;

/**
 * Class Config
 *
 * @package MongoOdm
 */
class Config
{
    /** @var string */
    protected $host;

    /** @var string */
    protected $port;

    /** @var string */
    protected $database;

    /** @var string */
    protected $username;

    /** @var string */
    protected $password;

    /** @var array */
    protected $options;

    /**
     * @return string
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * @param string $host
     *
     * @return \Tajawal\MongoOdm\Config
     */
    public function setHost($host)
    {
        $this->host = $host;

        return $this;
    }

    /**
     * @return string
     */
    public function getPort()
    {
        return $this->port;
    }

    /**
     * @param string $port
     *
     * @return \Tajawal\MongoOdm\Config
     */
    public function setPort($port)
    {
        $this->port = $port;

        return $this;
    }

    /**
     * @return string
     */
    public function getDatabase()
    {
        return $this->database;
    }

    /**
     * @param string $database
     *
     * @return \Tajawal\MongoOdm\Config
     */
    public function setDatabase($database)
    {
        $this->database = $database;

        return $this;
    }

    /**
     * @return string
     */
    public function getUsername()
    {
        return $this->username;
    }

    /**
     * @param string $username
     *
     * @return \Tajawal\MongoOdm\Config
     */
    public function setUsername($username)
    {
        $this->username = $username;

        return $this;
    }

    /**
     * @return string
     */
    public function getPassword()
    {
        return $this->password;
    }

    /**
     * @param string $password
     *
     * @return \Tajawal\MongoOdm\Config
     */
    public function setPassword($password)
    {
        $this->password = $password;

        return $this;
    }

    public function setOptions(array $options)
    {
        $this->options = $options;

        return $this;
    }

    public function getOptions()
    {
        return $this->options;
    }
}
