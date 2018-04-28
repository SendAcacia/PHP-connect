<?php

/**
 * MongoDB操作类
 * <Author:>cankai</Author:>
 * <Date:>2018/4/19</Date:>
 */
 class MongoConnection
 {
    /**
     * MONGO实例对象
     */
    private static $ins = NULL;

    /**
     * MONGO连接对象
     */
    private $_conn = NULL;

    /**
     * MONGO数据库名
     */
    private $_dbname = NULL;

    /**
     * Where条件数组
     */
    private $where = array();

    /**
     * 查询选项
     */
    private $options = array();

    /**
     * 当前操作的集合名
     */
    private $table = '';

    /**
     * 升序
     */
    private $asc = 1;

    /**
     * 降序
     */
    private $desc = -1;

    /**
     * Bulk
     */
    private $bulk = NULL;

    /**
     * 写入模式
     */
    private $writeConcern = NULL; 

    /**
     * 私有构造方法，防止实例化
     */
    private function __construct() {
        $this->_conn        = new MongoDB\Driver\Manager('mongodb://' . MONGO_USER . ':' . MONGO_PWD . '@' . MONGO_HOST . ':' . MONGO_PORT);
        $this->_dbname      = MONGO_DBNAME;
        $this->writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);
    }

    /**
     * 单例模式
     */
    public static function instance() {
        is_null(self::$ins) AND self::$ins = new self();
        return self::$ins;
    }

    /**
     * 私有克隆方法，防止外面克隆
     */
    private function _clone() {}

    /**
     * 建立一个新的数据库连接，单次有效
     * @param  string  $dbname     数据库名称
     */
    public function connect($dbname) {
        $this->_dbname = $dbname;
    }

    /**
     * 像一个集合里面插入一条记录
     * @param  string $table 集合名
     * @param  mixed  $data  要插入的数据
     * @return mixed         插入的ID
     */
    public function insert($table, $data) {
        if ( empty($data) ) return FALSE;
    	$this->table = $table; 
    	$document    = $data;
        
        $this->bulk  = new MongoDB\Driver\BulkWrite;

    	!isset($document['_id']) AND $document['_id'] = new MongoDB\BSON\ObjectID;
    	$this->bulk->insert($document);
        $result = $this->_conn->executeBulkWrite($this->_dbname . '.' . $this->table, $this->bulk, $this->writeConcern);
        
		$this->reflush();
		if ( $result ) {
			return (string)$document['_id'];
		} else {
			return FALSE;
		}
    }

    /**
     * 批量插入多条数据
     * @param  string  $table 集合名
     * @param  mixed   $data  要插入的数据
     * @return integer        插入的条数
     */
    public function insertAll($table, $data) {
        if ( empty($data) ) return FALSE;
        $this->table = $table;

        $this->bulk  = new MongoDB\Driver\BulkWrite;

        foreach ( $data as $document ) {
            !isset($document['_id']) AND $document['_id'] = new MongoDB\BSON\ObjectID;
    	    $this->bulk->insert($document);
        }

        $result = $this->_conn->executeBulkWrite($this->_dbname . '.' . $this->table, $this->bulk, $this->writeConcern);
        $this->reflush();
        if ( $result ) {
			return $result->getInsertedCount();
		} else {
			return FALSE;
		}
    }

    /**
     * 更新操作
     * @param  string $table 集合名
     * @param  mixed  $data  需要更新的数据
     * @return number        影响的行数
     */
    public function update($table, $data) {
        if ( empty($data) ) return FALSE;
        $this->table = $table; 
        $this->bulk  = new MongoDB\Driver\BulkWrite;
    	$this->bulk->update(
    		$this->where,
    		['$set' => $data],
    		['multi' => false, 'upsert' => false]
    	);

		$result = $this->_conn->executeBulkWrite($this->_dbname . '.' . $this->table, $this->bulk, $this->writeConcern);
		$this->reflush();
        if ( $result ) {
            return $result->getModifiedCount();
        } else {
            return FALSE;
        }
    }

    /**
     * 批量保存
     * @param   string   $table    集合名
     * @param   mixed    $data     需要更新的数据
     * @param   string   $filed    辨别的键名
     * @return  
     */
    public function saveAll($table, $data, $field = '_id') {
        if ( empty($data) || gettype($data) != 'array' ) return FALSE;
        // $this->table = $table;
        $this->bulk  = new MongoDB\Driver\BulkWrite;
        foreach ( $data as $item ) {
            if ( $this->where($field, $item[$field])->findOne($table) ) {
                $this->bulk->update([$field=>$item[$field]], ['$set'=>$item]);
            } else {
    	        $this->bulk->insert($item);
            }
        }

        $result = $this->_conn->executeBulkWrite($this->_dbname . '.' . $table, $this->bulk, $this->writeConcern);
        $this->reflush();

        if ( $result ) {
            return $result->getModifiedCount() + $result->getInsertedCount();
        } else {
            return FALSE;
        }
    }

    /**
     * 删除记录
     * @param  string  $table 集合名
     * @param  boolean $limit limit 为 1 时，删除第一条匹配数据；limit 为 0 时，删除所有匹配数据；默认为0
     * @return integer        删除的行数
     */
    public function delete($table, $limit = 0) {
    	$this->table = $table; 
        $this->bulk  = new MongoDB\Driver\BulkWrite;
		$this->bulk->delete($this->where, ['limit' => $limit]);

		$result = $this->_conn->executeBulkWrite($this->_dbname . '.' . $this->table, $this->bulk, $this->writeConcern);
        $this->reflush();
        
        if ( $result ) {
            return $result->getDeletedCount();
        } else {
            return FALSE;
        }
    }

    /**
     * 查询所有匹配的结果集
     * @param  string  $table  集合名
     * @return array           所有匹配的结果集
     */
    public function find($table = NULL) {
    	!is_null($table) AND $this->table = $table; 
    	$query = new MongoDB\Driver\Query($this->where, $this->options);
		$cursor = $this->_conn->executeQuery($this->_dbname . '.' . $this->table, $query);
		$this->reflush();

		$result = [];
		foreach ( $cursor as $document ) {
			$result[] = (array)$document;
		}

		return $result;
    }

    /**
     * 查询匹配的第一条结果集
     * @param  string  $table  集合名
     * @return array 匹配的第一条结果集
     */
    public function findOne($table = NULL) {
    	!is_null($table) AND $this->table = $table;

    	if ( isset($this->options['skip']) ) unset($this->options['skip']);
    	$this->options['limit'] = 1;

    	$query = new MongoDB\Driver\Query($this->where, $this->options);
		$cursor = $this->_conn->executeQuery($this->_dbname . '.' . $this->table, $query);
		$this->reflush();

		$result = [];
		foreach ( $cursor as $document ) {
			$result = (array)$document;
		}

		return $result;
    }

    /**
     * 计算出返回结果总条数
     * @param   string     $table    表名
     * @return  number               结果条数
     */
    public function count_all($table) {
        $collection = new MongoDB\Collection($this->_conn, $this->_dbname, $table);
        $this->reflush();
        $rows = $collection->count($this->where);
        
        return $rows;
    }

    /**
     * where条件查询
     * @param  mixed  $field 可以是字段，也可以是查询的数组
     * @param  mixed  $value 如果$field为字段，那么为字段的值
     * @return self          返回自身
     */
    public function where($field, $value = NULL) {
    	switch (gettype($field)) {
    		case 'string':
    			$this->where[$field] = $value;
    		break;

    		case 'array':
    			$this->where = $field;
    		break;
    		
    		default:
    			return false;
    		break;
    	}
    	return $this;
    }

    /**
     * Where or 条件查询
     * @param   array   $data     where条件集合
     * @return  self
     */
    public function where_or($data) {
        if ( gettype($data) != 'array' ) return FALSE;
        $this->where['$or'] = $data;
        return $this;
    }

    /**
     * Where In 条件查询
     * @param   mixed    $field   字段
     * @param   mixed    $data    字段值
     * @return  self
     */
    public function where_in($field, $data = NULL) {
        if ( is_null($data) ) {
            foreach ( $field as $key => $item ) {
                $this->where[$key] = ['$in'=>$item];
            }
        } else {
            $this->where[$field] = ['$in'=>$data];
        }
        return $this;
    }

    /**
     * 选择查询的字段
     * @param  string $field 字段字符串
     * @return self
     */
    public function select($field) {
    	$fields = explode(',', $field);
    	!in_array('_id', $fields) AND $this->options['projection']['_id'] = 0;
    	foreach ( $fields as $key ) {
    		$this->options['projection'][$key] = 1;
    	}

    	return $this;
    }

    /**
     * From
     * @param  string $table 集合名
     * @return self
     */
    public function from($table) {
    	$this->table = $table;
    	return $this;
    }

    /**
     * 排序
     * @param  mixed  $field 排序字段
     * @param  string $sort  排序规则
     * @return self
     */
    public function order_by($field, $sort = NULL) {
    	$sort = strtolower($sort);
    	switch (gettype($field)) {
    		case 'string':
    			$this->options['sort'][$field] = $this->$sort;
    		break;

    		case 'array':
    			$this->options['sort'] = array_map(function($sort){
    				$sort = strtolower($sort);
    				return $this->$sort;
    			}, $field);
    		break;
    		
    		default:
    			return false;
    		break;
    	}

    	return $this;
    }

	/**
	 * 限制返回条目
	 * @param  integer $offset 起始位置
	 * @param  integer $limit  条目数
	 * @return self
	 */
    public function limit($offset, $limit = NULL) {
    	if ( !is_null($limit) ) {
    		$this->options['limit'] = $limit;
    		$this->options['skip']  = $offset;
    	} else {
    		$this->options['limit'] = $offset;
    	}
    	return $this;
    }

    /**
     * 重置属性
     * @return void
     */
    private function reflush() {
    	$this->table   = '';
        $this->where   = array();
        $this->_dbname = MONGO_DBNAME;
    	$this->options = array();
    }

 }