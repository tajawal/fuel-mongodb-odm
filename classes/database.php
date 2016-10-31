<?php

namespace MongoOdm;

use Exception;
use MongoClient;
use MongoCode;
use MongoCollection;
use MongoDB;
use MongoException;

/**
 * This class wraps the functionality of Mongo (connection) and MongoDB (database object) into one class.
 *
 * <code>
 *  $db = Database::instance();
 * </code>
 *
 * The above will assume the 'default' configuration from the APPPATH/config/mongo.php file.
 * Alternatively it may be instantiated with the name and configuration specified as arguments:
 *
 * <code>
 *   $db = Database::instance('test', array(
 *     'database' => 'test'
 *   ));
 * </code>
 *
 * The Collection class will gain access to the server by calling the instance method with a configuration name,
 * so if the configuration name is not present in the config file then the instance should be
 * created before using any classes that extend Collection or Mongo_Document.
 *
 * Database can proxy all methods of MongoDB to the database instance as well as select collections using the
 * __get magic method.
 *
 * @method array authenticate(string $username, string $password)
 * @method array command(array $data, array $options = [])
 * @method MongoCollection createCollection(string $name, array $options = [])
 * @method array createDBRef(mixed $ns, mixed $a)
 * @method array drop()
 * @method array dropCollection(mixed $coll)
 * @method array execute(mixed $code, array $args = [])
 * @method bool forceError()
 * @method array getCollectionNames(bool $includeSystemCollections = false)
 * @method array getDBRef(array $ref)
 * @method int getProfilingLevel()
 * @method array getReadPreference()
 * @method bool getSlaveOkay()
 * @method array lastError()
 * @method array listCollections(bool $includeSystemCollections = false)
 * @method array prevError()
 * @method array repair(bool $preserve_cloned_files = false, bool $backup_original_files = false)
 * @method array resetError()
 * @method int setProfilingLevel(int $level)
 * @method bool setReadPreference(int $read_preference, array $tags = [])
 * @method bool setSlaveOkay(bool $ok = true)
 *
 * @author  Colin Mollenhour
 * @package MongoOdm
 *
 * This class was adapted from http://github.com/Wouterrr/MangoDB
 */
class Database
{
    /* See http://bsonspec.org */
    const TYPE_DOUBLE      = 1;
    const TYPE_STRING      = 2;
    const TYPE_OBJECT      = 3;
    const TYPE_ARRAY       = 4;
    const TYPE_BINARY      = 5;
    const TYPE_OBJECTID    = 7;
    const TYPE_BOOLEAN     = 8;
    const TYPE_DATE        = 9;
    const TYPE_NULL        = 10;
    const TYPE_REGEX       = 11;
    const TYPE_CODE        = 13;
    const TYPE_SYMBOL      = 14;
    const TYPE_CODE_SCOPED = 15;
    const TYPE_INT32       = 16;
    const TYPE_TIMESTAMP   = 17;
    const TYPE_INT64       = 18;
    const TYPE_MIN_KEY     = 255;
    const TYPE_MAX_KEY     = 127;

    /** Database instances
     *
     * @static  array
     */
    protected static $instances = [];

    /**
     * Get a Database instance. Configuration options are:
     *
     * <pre>
     *  server      A server connection string. See Mongo::__construct()
     *  options     The additional options for the connection ("connect" and "persist")
     *  database    *required* The database name to use for this instance
     * </pre>
     *
     * @param   string         $name The configuration name
     *
     * @param \MongoOdm\Config $config
     *
     * @return \MongoOdm\Database
     * @throws \Exception
     * @static
     */
    public static function instance($name = 'default', Config $config = null)
    {
        if (\array_key_exists($name, static::$instances)) {
            return static::$instances[$name];
        }

        if (empty($config)) {
            throw new Exception('Configuration is required when not instantiated already');
        }

        $config = [
            'hostname' => $config->getHost(),
            'port'     => $config->getPort(),
            'database' => $config->getDatabase(),
            'username' => $config->getUsername(),
            'password' => $config->getPassword(),
            'options'  => $config->getOptions(),
        ];

        static::$instances[$name] = new static($name, $config);

        return static::$instances[$name];
    }

    /** Database instance name
     *
     * @var  string
     */
    protected $_name;

    /** Database hostname
     *
     * @var  string
     */
    protected $_hostname;

    /** Database port number
     *
     * @var  string
     */
    protected $_port;

    /** Connection state
     *
     * @var  boolean
     */
    protected $_connected = false;

    /** The Mongo server connection
     *
     * @var  MongoClient
     */
    protected $_connection;

    /** The database instance for the database name chosen by the config
     *
     * @var  MongoDB
     */
    protected $_db;

    /** The class name for the MongoCollection wrapper. Defaults to Collection.
     *
     * @var string
     */
    protected $_collection_class;

    /** A flag to indicate if profiling is enabled and to allow it to be enabled/disabled on the fly
     *
     * @var  boolean
     */
    public $profiling;

    /** A callback called when profiling starts
     *
     * @var callback
     */
    protected $_start_callback = ['Profiler', 'start'];

    /** A callback called when profiling stops
     *
     * @var callback
     */
    protected $_stop_callback = ['Profiler', 'stop'];

    /**
     * This cannot be called directly, use Database::instance() instead to get an instance of this class.
     *
     * @param  string $name   The configuration name
     * @param  array  $config The configuration data
     */
    protected function __construct($name, array $config)
    {
        $this->_name = $name;

        // Setup connection options merged over the defaults and store the connection
        $options = [
            'connect' => false  // Do not connect yet
        ];

        if (isset($config['options'])) {
            $options = array_merge($options, $config['options']);
        }

        // Use the default server string if no server option is given
        empty($config['hostname']) and $config['hostname'] = ini_get('mongo.default_host');
        empty($config['port']) and $config['port'] = ini_get('mongo.default_port');

        $connection_string = "mongodb://";

        if (!empty($config['username']) and !empty($config['password'])) {
            $connection_string .= "{$config['username']}:{$config['password']}@";
        }

        if (isset($config['hosts']) && is_array($config['hosts'])) {
            $connection_string .= implode(',', $config['hosts']);
        } else {
            $connection_string .= "{$config['hostname']}:{$config['port']}";
            $this->_hostname = $config['hostname'];
            $this->_port     = $config['port'];
        }

        $connection_string .= "/{$config['database']}";

        $this->_connection = new MongoClient($connection_string, $options);

        // Save the database name for later use
        $this->_db = $config['database'];

        // Set the collection class name
        $this->_collection_class = (isset($config['collection']) ? $config['collection'] : 'Collection');

        // Store the database instance
        self::$instances[$name] = $this;
    }

    final public function __destruct()
    {
        try {
            $this->close();
            $this->_connection = null;
            $this->_connected  = false;
        } catch (Exception $e) {
            // can't throw exceptions in __destruct
        }
    }

    /**
     * @return  string  The configuration name
     */
    final public function __toString()
    {
        return $this->_name;
    }

    /**
     * Force the connection to be established.
     * This will automatically be called by any MongoDB methods that are proxied via __call
     *
     * @return boolean
     * @throws MongoException
     */
    public function connect()
    {
        if (!$this->_connected) {
            $this->_connected = $this->_connection->connect();

            $this->_db = $this->_connection->selectDB("$this->_db");
        }

        return $this->_connected;
    }

    /**
     * Close the connection to Mongo
     *
     * @return  boolean  if the connection was successfully closed
     */
    public function close()
    {
        if ($this->_connected) {
            $this->_connected = $this->_connection->close();
            $this->_db        = "$this->_db";
        }

        return $this->_connected;
    }

    public function get_db_name()
    {
        return $this->_name;
    }

    public function get_hostname()
    {
        return $this->_hostname;
    }

    public function get_port()
    {
        return $this->_port;
    }

    /**
     * Expose the MongoDb instance directly.
     *
     * @return  MongoDb
     */
    public function db()
    {
        $this->_connected OR $this->connect();

        return $this->_db;
    }

    /**
     * Proxy all methods for the MongoDB class.
     * Profiles all methods that have database interaction if profiling is enabled.
     * The database connection is established lazily.
     *
     * @param  string $name
     * @param  array  $arguments
     *
     * @return mixed
     * @throws \Exception
     */
    public function __call($name, $arguments)
    {
        $this->_connected OR $this->connect();

        if (!method_exists($this->_db, $name)) {
            throw new Exception("Method does not exist: MongoDb::$name");
        }

        $retval = call_user_func_array([$this->_db, $name], $arguments);

        return $retval;
    }

    /**
     * Same usage as MongoDB::execute except it throws an exception on error
     *
     * @param  string $code
     * @param  array  $args
     * @param  array  $scope A scope for the code if $code is a string
     *
     * @return mixed
     * @throws MongoException
     */
    public function execute_safe($code, array $args = [], $scope = [])
    {
        if (!$code instanceof MongoCode) {
            $code = new MongoCode($code, $scope);
        }
        $result = $this->execute($code, $args);
        if (empty($result['ok'])) {
            throw new MongoException($result['errmsg'], $result['errno']);
        }

        return $result['retval'];
    }

    /**
     * Run a command, but throw an exception on error
     *
     * @param array $command
     *
     * @return array
     * @throws MongoException
     */
    public function command_safe($command)
    {
        $result = $this->command($command);
        if (empty($result['ok'])) {
            $message = isset($result['errmsg']) ? $result['errmsg'] : 'Error: ' . json_encode($result);
            $code    = isset($result['errno']) ? $result['errno'] : 0;
            throw new MongoException($message, $code);
        }

        return $result;
    }

    /**
     * Get a Collection instance (wraps MongoCollection)
     *
     * @param  string $name
     *
     * @return Collection
     */
    public function selectCollection($name)
    {
        $this->_connected OR $this->connect();

        return new $this->_collection_class($name, $this->_name);
    }

    /**
     * Get a Collection instance with grid FS enabled (wraps MongoCollection)
     *
     * @param  string $prefix
     *
     * @return Collection
     */
    public function getGridFS($prefix = 'fs')
    {
        $this->_connected OR $this->connect();

        return new $this->_collection_class($prefix, $this->_name, true);
    }

    /**
     * Fetch a collection by using object access syntax
     *
     * @param  string $name The collection name to select
     *
     * @return  Collection
     */
    public function __get($name)
    {
        return $this->selectCollection($name);
    }

    /**
     * Simple findAndModify helper
     *
     * @param string $collection
     * @param array  $command
     *
     * @return array
     * @throws MongoException
     */
    public function findAndModify($collection, $command)
    {
        $command = array_merge(['findAndModify' => (string)$collection], $command);
        $result  = $this->command_safe($command);

        return $result['value'];
    }

    /**
     * Get the next auto-increment value for the given key
     *
     * @param        $key
     * @param string $collection
     *
     * @return int
     */
    public function get_auto_increment($key, $collection = 'autoincrements')
    {
        $data = $this->findAndModify($collection, [
            'query'  => ['_id' => $key],
            'update' => ['$inc' => ['value' => 1]],
            'upsert' => true,
            'new'    => true,
        ]);

        return $data['value'];
    }

    /**
     * Allows one to override the default Collection class.
     *
     * @param string $class_name
     */
    public function set_collection_class($class_name)
    {
        $this->_collection_class = $class_name;
    }
}
