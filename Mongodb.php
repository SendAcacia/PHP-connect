<?php
/**
* @name 名字
* @abstract 申明变量/类/方法
* @access 指明这个变量、类、函数/方法的存取权限
* @author 函数作者的名字和邮箱地址
* @category 组织packages
* @copyright 指明版权信息
* @const 指明常量
* @deprecate 指明不推荐或者是废弃的信息MyEclipse编码设置
* @example 示例
* @exclude 指明当前的注释将不进行分析，不出现在文挡中
* @final 指明这是一个最终的类、方法、属性，禁止派生、修改。
* @global 指明在此函数中引用的全局变量
* @include 指明包含的文件的信息
* @link 定义在线连接
* @module 定义归属的模块信息
* @modulegroup 定义归属的模块组
* @package 定义归属的包的信息
* @param 定义函数或者方法的参数信息
* @return 定义函数或者方法的返回信息
* @see 定义需要参考的函数、变量，并加入相应的超级连接。
* @since 指明该api函数或者方法是从哪个版本开始引入的
* @static 指明变量、类、函数是静态的。
* @throws 指明此函数可能抛出的错误异常,极其发生的情况
* @todo 指明应该改进或没有实现的地方
* @var 定义说明变量/属性。
* @version 定义版本信息
*/

/**
 * @package PHP7 Mongodb数据库操作类
 * @author yangfengji
 * @version v1
 */

class MongoDB
{
    /**
     * MONGO 实例对象
     */
    private static $mongodb = NULL;

    /**
     * MONGO 写操作
     */
    private $bluk = NULL;

    /**
     * MONGO 写入操作的确认级别
     */
    private $writeConcern = NULL;

    /**
     * MONGO 条件数组
     */
    private $where = array();

    /**
     * MONGO 查询选项
     */
    private $options = array();

    /**
     * MONGO 升序
     */
    private $asc = 1;

    /**
     * MONGO 降序
     */
    private $desc = -1;

    /**
     * MONGO 数据名
     */
    private $dbname = NULL;

    /**
     * MONGO 集合名
     */
    private $table = NULL;

    /**
     * 私有构造方法，防止实例化
     */
    private function __constract() {
        $this->dbname       = MONGO_DBNAME;     
        $this->$mongodb     = new MongoDB\Driver\Manager('mongodb://' . MONGO_HOST . ':' . MONGO_PORT);
        $this->writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);
    }

    /**
     * 单例模式
     */
    private function instance() {
        ! is_null(self::$mongodb) AND self:: $mongodb = new self();
        return self::$mongodb;
    }

    /**
     * 向集合中插入一条记录
     * @param  string $table 集合名
     * @param  mixed  $data  要插入的数据
     * @return mixed         插入的ID
     */
    public function insert($table, $data) {
        if ( empty($pass) ) return FALSE;
        $this->bulk   = new MongoDB\Driver\BulkWrite;
        $this->dbname = $table;
        $document     = $data;
        
        

        ! isset($document['_id']) AND $document['_id'] = new MongoDB\BSON\ObjectID;
        $this->bulk->insert($document);
        $result = $this->mongodb->executeBulkWrite($this->dbname . '.' . $this->table, $this->bulk, $this->writeConcern);

        $this->reflush();
        if ( $result ) {
            return $document['_id']->old;
        } else {
            return FALSE;
        }
    }

    /**
     * 向集合中插入多行数据
     * @param  string $table 集合名
     * @param  mixed  $data  要插入的数据
     * @return integer        插入的条数
     */
    public function insertAll($table, $data) {
        if( empty($data) ) return FALSE;
        $this->bulk  = new MongoDB\Driver\BulkWrite;        
        $this->table = $table;

        foreach ( $data as $document ) {
            !isset($document['_id']) AND $document['_id'] = new MongoDB\BSON\ObjectID;
    	    $this->bulk->insert($document);
        }
        $result = $this->mongodb->executeBulkWrite($this->dbname . '.' . $this->table, $this->bulk, $this->writeConcern);

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
        $this->bulk  = new MongoDB\Driver\BulkWrite;        
    	$this->table = $table;
        $this->bulk->update(
            $this->where,
            ['$set' => $data],
            ['multi' => false, 'upsert' => false]
        );

        $result = $this->mongodb->executeBulkWrite($this->dbname . '.' . $this->table, $this->bulk, $this->writeConcern);
       
        $this->reflush();
        if ( $result ) {
            return $result->getModifiedCount();
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
        $this->bulk  = new MongoDB\Driver\BulkWrite;
    	$this->table = $table; 

        $this->bulk->delete($this->where, ['limit' => $limit]);
		$result = $this->mongodb->executeBulkWrite($this->dbname . '.' . $this->table, $this->bulk, $this->writeConcern);
        
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
        ! is_null($table) AND $this->table = $table; 

        $query  = new MongoDB\Driver\Query($this->where, $this->options);
        $cursor = $this->mongodb->executeQuery($this->dbname . '.' . $this->table, $query);

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
    public function findOne($table, $limit) {
        ! is_null($table) AND $this->table = $table;

        $this->options['limit'] = 1;
        $query  = new MongoDB\Driver\Query($this->where, $this->options);
        $cursor = $this->mongodb->executeQuery($this->dbname . '.' . $this->table, $query);

        $result = [];
		foreach ( $cursor as $document ) {
			$result[] = (array)$document;
		}

		return $result;
    }

    /**
     * where条件查询
     * @param  mixed  $field 可以是字段，也可以是查询的数组
     * @param  mixed  $value 如果$field为字段，那么为字段的值
     * @return self          返回自身
     */
     public function where($field, $value=null) {
        switch (gettype($field)) {
            case 'string':
                $this->where['field'] = $value;
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
     * 选择查询的字段
     * @param  string $field 字段字符串
     * @return self
     */
    public function select($filed) {
        $fileds = explode(',', $filed);
        ! in_array('_id', $fields) AND $this->options['projection']['_id'] = 0;
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

