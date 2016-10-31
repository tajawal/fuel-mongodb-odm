<?php

namespace Tajawal\MongoOdm;

use Exception;
use MongoCollection;
use MongoConnectionException;
use MongoCursor;
use MongoCursorException;
use MongoDB\BSON\Javascript;
use MongoException;
use MongoDB\BSON\ObjectID;

/**
 * This class can be used in any of the following ways:
 *
 * 1. Directly as a wrapper for MongoCollection/MongoCursor:
 * <code>
 *     $posts = new Collection('posts');
 *     $posts->sort_desc('published')->limit(10)->as_array(); // array of arrays
 * </code>
 *
 * 2. As part of the Table Data Gateway pattern
 * <code>
 * class Model_Post extends Document {
 *   protected $name = 'posts';
 *   // All model-related code here
 * }
 *
 * $posts = Document::factory('post')->collection(TRUE);
 * $posts->sort_desc('published')->limit(10)->as_array(); // array of Model_Post
 * </code>
 *
 * 3. As part of the Row Data Gateway pattern:
 * <code>
 * class Model_Post_Collection extends Collection {
 *   protected $name = 'posts';
 *   // Collection-related code here
 * }
 *
 * class Model_Post extends Document {
 *   // Document-related code here
 * }
 *
 * $posts = Document::factory('post')->collection(TRUE);
 * $posts->sort_desc('published')->limit(10)->as_array(); // array of Model_Post
 * </code>
 *
 * @method mixed batchInsert(array $a, array $options = [])
 * @method array createDBRef(array $a)
 * @method array deleteIndex(mixed $keys)
 * @method array deleteIndexes()
 * @method array drop()
 * @method bool ensureIndex(mixed $keys, array $options = [])
 * @method array getDBRef(array $ref)
 * @method array getIndexInfo()
 * @method string getName()
 * @method array getReadPreference()
 * @method bool getSlaveOkay()
 * @method array group(mixed $keys, array $initial, Javascript $reduce, array $options = [])
 * @method bool|array insert(array $data, array $options = [])
 * @method bool|array remove(array $criteria = [], array $options = [])
 * @method mixed save(array $a, array $options = [])
 * @method bool setReadPreference(int $read_preference, array $tags = [])
 * @method bool setSlaveOkay(bool $ok = true)
 * @method bool|array update(array $criteria, array $new_object, array $options = [])
 * @method array validate(bool $scan_data = false)
 *
 * @author  Colin Mollenhour
 * @package MongoOdm
 */
class Collection implements \Iterator, \Countable
{
    const ASC  = 1;
    const DESC = -1;

    /**
     * Instantiate an object conforming to Collection conventions.
     *
     * @param   string $name The model name to instantiate
     *
     * @return  Collection
     * @deprecated
     */
    public static function factory($name)
    {
        return Document::factory($name)->collection(true);
    }

    /** The name of the collection within the database or the gridFS prefix if gridFS is TRUE
     *
     * @var  string
     */
    protected $name;

    /** The database configuration name (passed to Database::instance() )
     *
     * @var  string
     */
    protected $db = 'default';

    /** Whether or not this collection is a gridFS collection
     *
     * @var  bool
     */
    protected $gridFS = false;

    /** The class name or instance of the corresponding document model or NULL if direct mode
     *
     * @var  string
     */
    protected $_model;

    /** The cursor instance in use while iterating a collection
     *
     * @var  MongoCursor
     */
    protected $_cursor;

    /** The current query criteria (with field names translated)
     *
     * @var  array
     */
    protected $_query = [];

    /** The current query fields (a hash of 'field' => 1)
     *
     * @var  array
     */
    protected $_fields = [];

    /** The current query options
     *
     * @var  array
     */
    protected $_options = [];

    /** A cache of MongoCollection instances for performance
     *
     * @static  array
     */
    protected static $collections = [];

    /** A cache of Document model instances for performance
     *
     * @static  array
     */
    protected static $models = [];

    /**
     * Instantiate a new collection object, can be used for querying, updating, etc..
     *
     * @param  string      $name   The collection name
     * @param  string      $db     The database configuration name
     * @param  bool        $gridFS Is the collection a gridFS instance?
     * @param  bool|string $model  Class name of template model for new documents
     */
    public function __construct($name = null, $db = 'default', $gridFS = false, $model = false)
    {
        if ($name !== null) {
            $this->db     = $db;
            $this->name   = $name;
            $this->gridFS = $gridFS;
        }
        if ($model) {
            $this->_model = $model;
        }
    }

    /**
     * Cloned objects have uninitialized cursors.
     */
    public function __clone()
    {
        $this->reset(true);
    }

    /**
     * Reset the state of the query (must be called manually if re-using a collection for a new query)
     *
     * @param bool $cursor_only
     *
     * @return  Collection
     */
    public function reset($cursor_only = false)
    {
        if (!$cursor_only) {
            $this->_query = $this->_fields = $this->_options = [];
        }
        $this->_cursor = null;

        return $this;
    }

    /**
     * Magic method override. Passes on method calls to either the MongoCursor or the MongoCollection
     *
     * @param   string $name
     * @param   array  $arguments
     *
     * @return mixed
     * @throws Exception
     * @throws MongoConnectionException
     */
    public function __call($name, $arguments)
    {
        if ($this->_cursor && method_exists($this->_cursor, $name)) {
            return call_user_func_array([$this->_cursor, $name], $arguments);
        }

        if (method_exists($this->collection(), $name)) {
            try {
                $return = call_user_func_array([$this->collection(), $name], $arguments);
            } catch (MongoConnectionException $exception) {
                throw $exception;
            }

            return $return;
        }

        throw new Exception('Method not found by MongoOdm\Collection: ' . $name);
    }

    /**
     * Get the MongoOdm\Database instance used for this collection
     *
     * @return  Database
     */
    public function db()
    {
        return Database::instance($this->db);
    }

    /**
     * Get the corresponding MongoCollection instance
     *
     * @return  MongoCollection
     */
    public function collection()
    {
        $name = "$this->db.$this->name.$this->gridFS";
        if (!isset(self::$collections[$name])) {
            $selectMethod             = ($this->gridFS ? 'getGridFS' : 'selectCollection');
            self::$collections[$name] = $this->db()->db()->$selectMethod($this->name);
        }

        return self::$collections[$name];
    }

    /**
     * Set some criteria for the query. Unlike MongoCollection::find, this can be called multiple
     * times and the query parameters will be merged together.
     *
     * <pre>
     * Usages:
     *   $query is an array
     *   $query is a field name and $value is the value to search for
     *   $query is a JSON string that will be interpreted as the query criteria
     * </pre>
     *
     * @param   mixed $query An array of parameters or a key
     * @param   mixed $value If $query is a key, this is the value
     *
     * @return \Tajawal\MongoOdm\Collection
     * @throws \MongoCursorException
     * @throws \MongoException
     */
    public function find($query = [], $value = null)
    {
        if ($this->_cursor)
            throw new MongoCursorException('The cursor has already been instantiated.');
        if (!is_array($query)) {
            if ($query[0] == "{") {
                $query = Json::arr($query);
                if ($query === null) {
                    throw new \MongoException('Unable to parse query from JSON string.');
                }
            } else {
                $query = [$query => $value];
            }
        }

        // Translate field aliases
        $query_fields = [];
        foreach ($query as $field => $value) {
            // Special purpose condition
            if ($field[0] == '$') {
                // $or and $where and possibly other special values
                if ($field == '$or' && !is_int(key($value))) {
                    if (!isset($this->_query['$or'])) {
                        $this->_query['$or'] = [];
                    }
                    $this->_query['$or'][] = $value;
                } else if ($field == '$where') {
                    $this->_query['$where'] = $value;
                } else {
                    $query_fields[$field] = $value;
                }
            } // Simple key = value condition
            else {
                $query_fields[$this->get_field_name($field)] = $value;
            }
        }

        $this->_query = self::array_merge_recursive_distinct($this->_query, $query_fields);

        return $this;
    }

    /**
     * Add fields to be returned by the query.
     *
     * @param   array    $fields
     * @param   int|bool $include
     *
     * @return \Tajawal\MongoOdm\Collection
     * @throws \MongoCursorException
     */
    public function fields($fields = [], $include = 1)
    {
        if ($this->_cursor)
            throw new MongoCursorException('The cursor has already started iterating.');

        // Map array to hash
        if ($fields == array_values($fields)) {
            $fields = array_fill_keys($fields, (int)$include);
        }

        // Translate field aliases
        foreach ($fields as $field => $value) {
            $this->_fields[$this->get_field_name($field)] = $value;
        }

        return $this;
    }

    /**
     * Gives the database a hint about the query
     *
     * @param   array $key_pattern
     *
     * @return  Collection
     */
    public function hint(array $key_pattern)
    {
        return $this->set_option('hint', $key_pattern);
    }

    /**
     * Sets whether this cursor will timeout
     *
     * @param   bool $liveForever
     *
     * @return  Collection
     */
    public function immortal($liveForever = true)
    {
        return $this->set_option('immortal', $liveForever);
    }

    /**
     * Limits the number of results returned
     *
     * @param   int $num
     *
     * @return  Collection
     */
    public function limit($num)
    {
        return $this->set_option('limit', $num);
    }

    /**
     * Skips a number of results
     *
     * @param   int $num
     *
     * @return  Collection
     */
    public function skip($num)
    {
        return $this->set_option('skip', $num);
    }

    /**
     * Sets whether this query can be done on a slave
     *
     * @param   bool $okay
     *
     * @return  Collection
     */
    public function slaveOkay($okay = true)
    {
        return $this->set_option('slaveOkay', $okay);
    }

    /**
     * Use snapshot mode for the query
     *
     * @return  Collection
     */
    public function snapshot()
    {
        return $this->set_option('snapshot', null);
    }

    /**
     * Sorts the results by given fields
     *
     * @param   array|string $fields    A sort criteria or a key (requires corresponding $value)
     * @param   string|int   $direction The direction if $fields is a key
     *
     * @return \Tajawal\MongoOdm\Collection
     * @throws \MongoCursorException
     */
    public function sort($fields, $direction = self::ASC)
    {
        if ($this->_cursor)
            throw new MongoCursorException('The cursor has already started iterating.');

        if (!isset($this->_options['sort'])) {
            $this->_options['sort'] = [];
        }

        if (!is_array($fields)) {
            $fields = [$fields => $direction];
        }

        // Translate field aliases
        foreach ($fields as $field => $direction) {
            if (is_string($direction)) {
                if ($direction == 'asc' || $direction == '1')
                    $direction = self::ASC;
                else
                    $direction = self::DESC;
            }

            $this->_options['sort'][$this->get_field_name($field)] = $direction;
        }

        return $this;
    }

    /**
     * Sorts the results ascending by the given field
     *
     * @param   string $field The field name to sort by
     *
     * @return  Collection
     */
    public function sort_asc($field)
    {
        return $this->sort($field, self::ASC);
    }

    /**
     * Sorts the results descending by the given field
     *
     * @param   string $field The field name to sort by
     *
     * @return  Collection
     */
    public function sort_desc($field)
    {
        return $this->sort($field, self::DESC);
    }

    /**
     * Sets whether this cursor will be left open after fetching the last results
     *
     * @param   bool $tail
     *
     * @return  Collection
     */
    public function tailable($tail = true)
    {
        return $this->set_option('tailable', $tail);
    }

    /**
     * See if a cursor has an option to be set before executing the query.
     *
     * @param  string $name
     *
     * @return bool
     */
    public function has_option($name)
    {
        return array_key_exists($name, $this->_options);
    }

    /**
     * Get a cursor option to be set before executing the query.
     * Also supports retrieving 'query' and 'fields'.
     *
     * @param  string $name
     *
     * @return mixed
     */
    public function get_option($name)
    {
        if ($name == 'query') {
            return $this->_query;
        }
        if ($name == 'fields') {
            return $this->_fields;
        }

        return isset($this->_options[$name]) ? $this->_options[$name] : null;
    }

    /**
     * Set a cursor option. Will apply to currently loaded cursor if it has not started iterating.
     * Also supports setting 'query' and 'fields'.
     *
     * @param  string $name
     * @param  mixed  $value
     *
     * @return \Tajawal\MongoOdm\Collection
     * @throws \MongoCursorException
     * @throws \MongoException
     */
    public function set_option($name, $value)
    {
        if ($name != 'batchSize' && $name != 'timeout' && $this->is_iterating()) {
            throw new MongoCursorException('The cursor has already started iterating.');
        }

        if ($name == 'query') {
            $this->_query = $value;
        } else if ($name == 'fields') {
            $this->_fields = $value;
        } else {
            if ($this->_cursor) {
                if ($value === null)
                    $this->_cursor->$name();
                else
                    $this->_cursor->$name($value);
            }

            $this->_options[$name] = $value;
        }

        return $this;
    }

    /**
     * Unset a cursor option to be set before executing the query.
     *
     * @param  string $name
     *
     * @return \Tajawal\MongoOdm\Collection
     * @throws \MongoCursorException
     * @throws \MongoException
     */
    public function unset_option($name)
    {
        if ($this->is_iterating()) {
            throw new MongoCursorException('The cursor has already started iterating.');
        }
        unset($this->_options[$name]);

        return $this;
    }

    /**
     * Is the query executed yet?
     *
     * @return bool
     */
    public function is_loaded()
    {
        return !!$this->_cursor;
    }

    /**
     * Is the query iterating yet?
     *
     * @return bool
     * @throws \MongoException
     */
    public function is_iterating()
    {
        if (!$this->_cursor) {
            return false;
        }
        $info = $this->_cursor->info();
        if (!isset($info['started_iterating'])) {
            throw new \MongoException('Driver version >= 1.0.10 required.');
        }

        return $info['started_iterating'];
    }

    /**
     * Instantiates a cursor, after this is called the query cannot be modified.
     * This is automatically called when the iterator initializes (rewind).
     *
     * @return \Tajawal\MongoOdm\Collection
     * @throws \MongoCursorException
     * @throws \MongoException
     */
    public function load()
    {
        // Execute the query, add query to any thrown exceptions
        try {
            $this->_cursor = $this->collection()->find($this->_query, $this->_fields);
        } catch (MongoCursorException $e) {
            throw new MongoCursorException("{$e->getMessage()}: {$this->inspect()}", $e->getCode());
        } catch (MongoException $e) {
            throw new \MongoException("{$e->getMessage()}: {$this->inspect()}", $e->getCode());
        }

        // Add cursor options
        foreach ($this->_options as $key => $value) {
            if ($value === null)
                $this->_cursor->$key();
            else
                $this->_cursor->$key($value);
        }

        return $this;
    }

    /**
     * Wrapper for MongoCollection#findOne which adds field name translations and allows query to be a single _id
     *
     * @param  mixed $query  An _id, a JSON encoded query or an array by which to search
     * @param  array $fields Fields of the results to return
     *
     * @return mixed Record matching query or NULL
     * @throws \Exception
     * @throws MongoConnectionException
     * @throws \MongoException
     */
    public function findOne($query = [], $fields = [])
    {
        // String query is either JSON encoded or an _id
        if (!is_array($query)) {
            if ($query[0] == "{") {
                $query = Json::arr($query);
                if ($query === null) {
                    throw new \MongoException('Unable to parse query from JSON string.');
                }
            } else {
                $query = ['_id' => $query];
            }
        }

        // Translate field aliases
        $query_trans = [];
        foreach ($query as $field => $value) {
            $query_trans[$this->get_field_name($field)] = $value;
        }

        $fields_trans = [];
        if ($fields && is_int(key($fields))) {
            $fields = array_fill_keys($fields, 1);
        }
        foreach ($fields as $field => $value) {
            $fields_trans[$this->get_field_name($field)] = $value;
        }

        return $this->__call('findOne', [$query_trans, $fields_trans]);
    }

    /**
     * Simple findAndModify helper
     *
     * @param array $command
     *
     * @return array
     */
    public function findAndModify($command)
    {
        return $this->db()->findAndModify($this->name, $command);
    }

    /**
     * Get the next auto-increment value for this collection
     *
     * @return int
     * @throws MongoException
     */
    public function get_auto_increment()
    {
        return $this->db()->get_auto_increment($this->name);
    }

    /**
     * Perform a group aggregation and return the result or throw an exception on error
     *
     * @param string|array      $keys
     * @param array             $initial
     * @param string|Javascript $reduce
     * @param array             $options
     *
     * @return mixed
     * @throws \Exception
     * @throws MongoConnectionException
     * @throws \MongoException on error
     */
    public function group_safe($keys, $initial, $reduce, $options = [])
    {
        if (is_string($keys)) {
            $keys = [$keys => 1];
        }
        if (!$reduce instanceof Javascript) {
            $reduce = new Javascript($reduce);
        }
        $result = $this->__call('group', [$keys, $initial, $reduce, $options]);
        if (empty($result['ok'])) {
            $message = json_encode($result); //isset($result['errmsg']) ? $result['errmsg'] : ;
            throw new \MongoException($message);
        }

        return $result['retval'];
    }

    /**
     * Perform an update, throw exception on errors.
     *
     * Return values depend on type of update:
     *   multiple     return number of documents updated on success
     *   upsert       return upserted id if upsert resulted in new document
     *   updatedExisting flag for all other cases
     *
     * @param array $criteria
     * @param array $update
     * @param array $options
     *
     * @return bool|int|ObjectID
     * @throws MongoException on error
     */
    public function update_safe($criteria, $update, $options = [])
    {
        $options = array_merge(['w' => 1, 'multiple' => false, 'upsert' => false], $options);
        $result  = $this->update($criteria, $update, $options);

        // In case 'safe' was overridden and disabled, just return the result
        if (isset($options['w']) && ($options['w'] === 0)) {
            return $result;
        }

        // According to the driver docs an exception should have already been thrown if there was an error, but just in case...
        if (!$result['ok']) {
            throw new \MongoException($result['err']);
        }
        if ($options['multiple']) {
            return $result['n'];
        } // Return the upserted id if a document was upserted with a new _id
        else if ($options['upsert'] && !$result['updatedExisting'] && isset($result['upserted'])) {
            return $result['upserted'];
        } // Return the updatedExisting flag for single, non-upsert updates
        else {
            return $result['updatedExisting'];
        }
    }

    /**
     * Remove, throw exception on errors.
     *
     * Returns number of documents removed if "safe", otherwise just if the operation was successfully sent.
     *
     * @param array $criteria
     * @param array $options
     *
     * @return bool|int
     * @throws MongoException on error
     */
    public function remove_safe($criteria, $options = [])
    {
        $options = array_merge(['w' => 1, 'justOne' => false], $options);
        $result  = $this->remove($criteria, $options);

        // In case 'safe' was overridden and disabled, just return the result
        if (isset($options['w']) && ($options['w'] === 0)) {
            return $result;
        }
        // According to the driver docs an exception should have already been thrown if there was an error, but just in case...
        if (!$result['ok']) {
            throw new MongoException($result['err']);
        }

        // Return the number of documents removed
        return $result['n'];
    }

    /**
     * Get an instance of the corresponding document model.
     *
     * @return  Document
     */
    protected function get_model()
    {
        if (!isset(self::$models[$this->_model])) {
            $model                       = $this->_model;
            self::$models[$this->_model] = new $model;
        }

        return self::$models[$this->_model];
    }

    /**
     * Translate a field name according to aliases defined in the model if they exist.
     *
     * @param  string $name
     *
     * @return string
     */
    public function get_field_name($name)
    {
        if (!$this->_model) {
            return $name;
        }

        return $this->get_model()->get_field_name($name);
    }

    /**
     * Access the MongoCursor instance directly, triggers a load if there is none.
     *
     * @return  MongoCursor
     */
    public function cursor()
    {
        $this->_cursor OR $this->load();

        return $this->_cursor;
    }

    /**
     * Returns the current query results as an array
     *
     * @param   bool $objects Pass FALSE to get raw data
     *
     * @return  array
     */
    public function as_array($objects = true)
    {
        $array = [];

        // Iterate using wrapper
        if ($objects) {
            foreach ($this as $key => $value) {
                $array[$key] = $value;
            }
        } // Iterate bypassing wrapper
        else {
            $this->rewind();
            foreach ($this->_cursor as $key => $value) {
                $array[$key] = $value;
            }
        }

        return $array;
    }

    /**
     * Return an array of values or an associative array of keys and values
     *
     * @param   string $key
     * @param   mixed  $val
     *
     * @return  array
     */
    public function select_list($key = '_id', $val = null)
    {
        if ($val === null) {
            $val = $key;
            $key = null;
        }

        $list = [];

        foreach ($this->cursor() as $data) {
            if ($key !== null) {
                $list[(string)$data[$key]] = (isset($data[$val]) ? $data[$val] : null);
            } else if (isset($data[$val])) {
                $list[] = $data[$val];
            }
        }

        return $list;
    }

    /**
     * Emulate an SQL "NATURAL JOIN" when there is a 1-1 or n-1 relationship with one additional query
     * for all related documents
     *
     * @param string $model_field
     * @param string $id_field
     *
     * @return array
     */
    public function natural_join($model_field, $id_field = null)
    {
        if (!$id_field) {
            $id_field = "_$model_field";
        }

        $left      = $this->as_array();
        $right_ids = [];
        foreach ($left as $doc) {
            $right_id = $doc->$id_field;
            if ($right_id) {
                $right_ids[$right_id] = true;
            }
        }
        if ($right_ids) {
            $right = $this->get_model()->$model_field->collection(true)
                                                     ->find([
                                                             '_id' => ['$in' => array_keys($right_ids)]]
                                                     )
                                                     ->as_array();
            foreach ($left as $doc) {
                if (isset($right[$doc->$id_field])) {
                    $doc->$model_field = $right[$doc->$id_field];
                }
            }
        }

        return $left;
    }

    /********************************
     * Iterator and Countable methods
     * *******************************/

    /**
     * Countable: count
     *
     * Count the results from the current query: pass FALSE for "all" results (disregard limit/skip)<br/>
     * Count results of a separate query: pass an array or JSON string of query parameters
     *
     * @param  mixed $query
     *
     * @return int
     * @throws \MongoCursorException
     * @throws \MongoException
     */
    public function count($query = true)
    {
        if (is_bool($query)) {
            $this->_cursor OR $this->load();

            $count = $this->_cursor->count($query);
        } else {
            if (is_string($query) && $query[0] == "{") {
                $query = Json::arr($query);
                if ($query === null) {
                    throw new MongoException('Unable to parse query from JSON string.');
                }
            }
            $query_trans = [];
            foreach ($query as $field => $value) {
                $query_trans[$this->get_field_name($field)] = $value;
            }

            $query = $query_trans;

            $count = $this->collection()->count($query);
        }

        return $count;
    }

    /**
     * Implement MongoCursor#hasNext to ensure that the cursor is loaded
     *
     * @return  bool
     */
    public function hasNext()
    {
        return $this->cursor()->hasNext();
    }

    /**
     * Implement MongoCursor#getNext so that the return value is a Document instead of array
     *
     * @return  array|Document
     */
    public function getNext()
    {
        $this->cursor()->next();

        return $this->current();
    }

    /**
     * Iterator: current
     *
     * @return array|Document
     */
    public function current()
    {
        $data = $this->_cursor->current();

        if (!$this->_model) {
            return $data;
        }
        $model = clone $this->get_model();

        return $model->load_values($data, true);
    }

    /**
     * Iterator: key
     *
     * @return string
     */
    public function key()
    {
        return $this->_cursor->key();
    }

    /**
     * Iterator: next
     */
    public function next()
    {
        $this->_cursor->next();
    }

    /**
     * Iterator: rewind
     */
    public function rewind()
    {
        try {
            $this->cursor()->rewind();
        } catch (MongoCursorException $e) {
            throw new \MongoCursorException("{$e->getMessage()}: {$this->inspect()}", $e->getCode());
        } catch (MongoException $e) {
            throw new \MongoException("{$e->getMessage()}: {$this->inspect()}", $e->getCode());
        }
    }

    /**
     * Iterator: valid
     *
     * @return bool
     */
    public function valid()
    {
        return $this->_cursor->valid();
    }

    /**
     * Return a string representation of the full query (in Mongo shell syntax)
     *
     * @return  string
     */
    public function inspect()
    {
        $query = [];
        if ($this->_query)
            $query[] = Json::str($this->_query);
        if ($this->_fields)
            $query[] = Json::str($this->_fields);
        $query = "db.$this->name.find(" . implode(',', $query) . ")";
        foreach ($this->_options as $key => $value) {
            $query .= ".$key(" . Json::str($value) . ")";
        }

        return $query;
    }

    /**
     * Return the collection name
     *
     * @return string
     */
    public function __toString()
    {
        return $this->name;
    }

    /**
     * array_merge_recursive_distinct does not change the datatypes of the values in the arrays.
     *
     * @param array $array1
     * @param array $array2
     *
     * @return array
     */
    protected static function array_merge_recursive_distinct(array $array1, array $array2)
    {
        if (!count($array1)) {
            return $array2;
        }

        foreach ($array2 as $key => $value) {
            if (is_array($value) && isset($array1 [$key]) && is_array($array1 [$key])) {
                // Intersect $in queries
                if ($key == '$in') {
                    $array1[$key] = array_intersect($array1[$key], $value);
                } // Union $nin and $all queries
                else if ($key == '$nin' || $key == '$all') {
                    $array1[$key] = array_unique(array_splice($array1[$key], count($array1[$key]), 0, $value));
                } // Recursively merge all other queries/values
                else {
                    $array1 [$key] = self::array_merge_recursive_distinct($array1 [$key], $value);
                }
            } else {
                $array1 [$key] = $value;
            }
        }

        return $array1;
    }

}
