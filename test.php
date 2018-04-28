<?php 

class DB
{
    /**
     * 数据库连接实例
     */
    private static $connect = NULL;

    /**
     * 数据库名称
     */
    private $dbname = NULL;

    /**
     * 数据库连接地址
     */
    private $dbhost = NULL;

    /**
     * 数据库连接用户名
     */
    private $dbuser = NULL;

    /**
     * 连接密码
     */
    private $dbpass = NULL;
    
    /**
     * 数据库类型
     */
    private $dbtype = NULL;

    /**
     * 连接参数
     */
    private $dblink = NULL;

    /**
     * 连接表数据
     */
    private $table = NULL;

    /**
     * where 条件
     */
    private $where = NULL;

    /**
     * 私有构造方法，防止外部实例化
     */
    private function __construct() {
        $this->dbhost = DB_HOST;
        $this->dbname = DB_NAME;
        $this->dbuser = DB_USER;
        $this->dbpass = DB_PASS;
        $this->dbtype = DB_TYPE;
        $this->dblink = "$this->dbtype:host=$this->dbhost;dbname=$this->dbname";
        $this->connect = new PDO($this->dblink, $this->dbuser, $this->dbpass);

    }

    /**
     * 单例模式
     */
    static function instance () {
        is_null(self::$connect) AND self::$connect = new self();
        return self::$connect;
    }

    /**
     * 私有克隆方法，防止外面克隆
     */
    private function _clone() {}

    /**
     * @package 执行查询类SQL并返回结果集对象
     * @param string $sql
     * @return mixed $rusult
     */
    public function query ($sql) {
        if ( empty($sql) ) return FALSE;
        
        $rusult = $this->connect->query($sql);
        return $this;
    }

    /**
     * @package 执行操作类SQL并返回受影响的记录数;
     * @param string $sql
     * @return mixed $rusult
     */
    public function exec ($sql) {
        if ( empty($sql) ) return FALSE;
       
        $rusult = $this->connect->exec($sql);
        return $this;
    }

    /**
     * @package 插入一行数据
     * @param   array $data
     * @return mixed
     */
    public function insert ($data) {
        if( empty($data) ) return FALSE;

        $key    = implode(',', array_keys($data));
        $value  = implode(',', array_values($data));
        $sql    = "INSERT INTO `$this->table` ($key) VALUES ($value)";
        $result = $this->exec($sql);
        
        return $result;
    } 

    /**
     * @package 插入多行数据
     * @param array $data
     * @return mixed  $rusult
     */
    public function insertAll ($data) {
        if( empty($data) ) return FALSE;

        $key   = '';
        $value = '';

        foreach($data as $k => $document){
            $key    = implode(',', array_keys($data));
            $value += '(' . implode(',', array_values($data)) . '),';
        }
        
        $value  = rtrim($value, ',');
        $sql    = "INSERT INTO `$this->table` ($key) VALUES $value";
        $result = $this->exec($sql);
        return $result;        

    }

    /**
     * @package 删除数据
     * @param array $float
     * @return mixed 
     */
    public function delete ($float) {
        if ( ! is_null($this->table) ) {

            if( ! is_null($this->where) ) {
                
                $sql = "DELETE FROM $this->table WHERE $this->where";
            }

            if ( is_null($this->where) && $float) {
                $sql = "DELETE FROM $this->table";
            }

        }

        $result = $this->exec($sql);
        return $result;
        
    }

    /**
     * @package 数据更新
     * @param array $data
     * @return mixed 
     */
    public function update ($data) {
        if( empty($data) ) return FALSE;

        $set = '';
        foreach($data as $key => $value) {
            $set += "`" . $key . "` = '" . $value . "'";
        }

        if ( is_null($this->where) ) {
            return FALSE;
        }
        $sql = "UPDATE `$this->table` SET $set WHERE $this->where";
        
        $result = $this->exec($sql);
        return $result;
 
    }

    /**
     * @package 查询一行数据
     * @return array $result
     */
    public function find () {
        $sql = "SELECT * FROM `$this->table` WHERE $this->where LIMIT 1";

        $result = $this->query($sql);
        return $result;
    }

    /**
     * @package 查询全部数据
     * @return array $result
     */
    public function select () {
        $sql = "SELECT * FROM `$this->table` WHERE $this->where";
        
        $result = $this->query($sql);
        return $result;
    }

    /**
     * @package 查询某一列的值
     * @param mixed $field
     * @return array $result
     */
    public function column ($field) {
        $column = NULL;
        switch (gettype($field)) {
            case 'string':
                $column = $field;
            break;
            case 'array':
                foreach($field as $value) {
                    $column += $value . ',' ;
                }
                $value  = rtrim($value, ',');
            break;
            default:
                return false;
    		break;
        }
        
        $sql = "SELECT $column FROM `$this->table` WHERE $this->where";
        
        $result = $this->query($sql);
        return $result;
    }

   /**
     *@package where条件查询
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
     * From
     * @param  string $table 表名
     * @return self
     */
    public function from($table) {
    	$this->table = $table;
    	return $this;
    }

    /**
     * 
     */
    public function order_by () {

    }








}