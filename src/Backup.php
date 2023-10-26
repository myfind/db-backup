<?php
namespace myfind\backup;

use think\facade\Db;
use think\facade\Config;

class Backup {

    /**
     * 文件指针
     * @var resource
     */
    private $fp;
    /**
     * 备份文件信息 part - 卷号，name - 文件名
     * @var array
     */
    private $file;
    /**
     * 当前打开文件大小
     * @var integer
     */
    private $size = 0;

    /**
     * 数据库配置
     * @var integer
     */
    private $dbconfig = array();


    /**
     * 备份配置
     * @var integer
     */
    private $config = array('path' => './BackupData/', //数据库备份路径
        'part' => 20971520, //数据库备份卷大小
        'compress' => 0, //数据库备份文件是否启用压缩 0不压缩 1 压缩
        'level' => 9, //数据库备份文件压缩级别 1普通 4 一般  9最高
    );

    /**
     * 数据库备份构造方法
     * @param array $file   备份或还原的文件信息
     * @param array $config 备份配置信息
     */
    public function __construct($config = []) {
        $this->config = array_merge($this->config, $config);
        //初始化文件名
        $this->setFile();
        //初始化数据库连接参数
        $this->setDbConn();
        //检查文件是否可写
        if (!$this->checkPath($this->config['path'])) {
            throw new \Exception("The current directory is not writable");
        }
    }


    /**
     * 设置脚本运行超时时间
     * 0表示不限制，支持连贯操作
     */
    public function setTimeout($time = null) {
        if (!is_null($time)) {
            set_time_limit($time) || ini_set("max_execution_time", $time);
        }
        return $this;
    }


    /**
     * 设置数据库连接必备参数
     * @param array $dbconfig 数据库连接配置信息
     * @return object
     */
    public function setDbConn($dbconfig = []) {
        if (empty($dbconfig)) {
            $this->dbconfig = Config::get('database');
        } else {
            $this->dbconfig = $dbconfig;
        }
        return $this;
    }

    //数据类连接
    public static function connect() {
        return Db::connect();
    }

    /***
     * 数据库表列表
     */
    public function DataList($table = null, $type = 1) {
        $db = self::connect();
        if (is_null($table)) {
            $list = $db->query("SHOW TABLE STATUS");
        } else {
            if ($type) {
                $list = $db->query("SHOW FULL COLUMNS FROM {$table}");
            } else {
                $list = $db->query("show columns from {$table}");
            }
        }
        return array_map('array_change_key_case', $list);
    }

    /***
     * 备份文件列表
     * file_name_path : 文件完整路径
     * file_name : 文件名称
     * create_time ： 文件的创建时间
     * file_size： 文件大小
     */
    public function FileList() {
        if (!is_dir($this->config['path'])) {
            mkdir($this->config['path'], 0777, true);
        }
        $path = realpath($this->config['path']);
        //迭代器遍历目录 :https://www.php.net/manual/zh/class.filesystemiterator.php 
        $glob = new \FilesystemIterator($path);
        $list = array();
        $i = 0;
        while ($glob->valid()) {
            // $glob->valid() 检测迭代器是否到底了
            $file_name_path = $path . '/' . $glob->getFilename();
            $list[$i]['file_name_path'] = $file_name_path;
            $list[$i]['file_name'] = $glob->getFilename();
            $list[$i]['create_time'] = $glob->getCTime();
            $list[$i]['file_size'] = filesize($file_name_path);
            $glob->next();  // 游标往后移动
            $i++;
        }
        return $list;
    }

    /***
     * 删除备份文件
     * @param string filename 文件名字
     */
    public function FileDel($filename) {
        $path = realpath($this->config['path']);
        $fileNamePath = $path . '/' . $filename;
        if (file_exists($fileNamePath)) {
            chmod($fileNamePath, 0777);
            unlink($fileNamePath);
            return true;
        } else {
            throw new \Exception("{$filename} 404");
        }
    }

    /**
     * 下载备份
     * @param string filename 文件名字
     */
    public function FileDownload($filename) {
        $path = realpath($this->config['path']);
        $fileNamePath = $path . '/' . $filename;
        if (file_exists($fileNamePath)) {
            //告诉浏览器这是一个文件流格式的文件   
            Header("Content-type: application/octet-stream;charset=utf-8");
            //用来告诉浏览器，文件是可以当做附件被下载，下载后的文件名称为$file_name该变量的值。
            Header("Content-Disposition: attachment; filename=" . $filename);
            //请求范围的度量单位
            Header("Accept-Ranges: bytes");
            //Content-Length是指定包含于请求或响应中数据的字节长度
            Header("Accept-Length: " . filesize($fileNamePath));
            readfile($fileNamePath);
            return true;
        } else {
            return false;
        }
    }

    /**
     * 设置备份文件名
     * @param Array $file 文件名字
     * @return object
     */
    public function SetFile($file = null) {
        if (is_null($file)) {
            $this->file = ['name' => date('YmdHis'), 'part' => 1];
        } else {
            if (!array_key_exists("name", $file) && !array_key_exists("part", $file)) {
                $this->file = $file['1'];
            } else {
                $this->file = $file;
            }
        }
        return $this;
    }

    /*
    *   备份表结构
    *   函数功能：把表的结构转换成为SQL   
    *   函数参数：$table: 要进行提取的表名   
    *   返 回 值：返回提取后的结果，SQL集合     
    */
    public function FileBackupTable($table, $start) {
        $db = self::connect();
        // 备份表结构
        if (0 == $start) {
            $result = $db->query("SHOW CREATE TABLE `{$table}`");
            $sql = "\n";
            $sql .= "-- -----------------------------\n";
            $sql .= "-- Table structure for `{$table}`\n";
            $sql .= "-- -----------------------------\n";
            $sql .= "DROP TABLE IF EXISTS `{$table}`;\n";
            $sql .= trim($result[0]['Create Table']) . ";\n\n";
            if (false === $this->write($sql)) {
                return false;
            }
        }
        //数据总数
        $result = $db->query("SELECT COUNT(*) AS count FROM `{$table}`");
        $count = $result['0']['count'];
        //备份表数据
        if ($count) {
            //还有更多数据
            if ($count > $start + 1000) {
                //return array($start + 1000, $count);
                return $this->FileBackupTable($table, $start + 1000);
            }
        }
        //备份下一表
        return true;
    }

    /**
     * 备份表结构+数据
     * @param string  $table 表名
     * @param integer $start 起始行数
     * @return boolean false - 备份失败
     */
    public function FileBackupData($table, $start) {
        $db = self::connect();

        // 备份表结构
        if (0 == $start) {
            $result = $db->query("SHOW CREATE TABLE `{$table}`");
            $sql = "\n";
            $sql .= "-- -----------------------------\n";
            $sql .= "-- Table structure for `{$table}`\n";
            $sql .= "-- -----------------------------\n";
            $sql .= "DROP TABLE IF EXISTS `{$table}`;\n";
            $sql .= trim($result[0]['Create Table']) . ";\n\n";
            if (false === $this->write($sql)) {
                return false;
            }
        }
        //数据总数
        $result = $db->query("SELECT COUNT(*) AS count FROM `{$table}`");
        $count = $result['0']['count'];
        //备份表数据
        if ($count) {
            //写入数据注释
            if (0 == $start) {
                $sql = "-- -----------------------------\n";
                $sql .= "-- Records of `{$table}`\n";
                $sql .= "-- -----------------------------\n";
                $this->write($sql);
            }
            //备份数据记录
            $result = $db->query("SELECT * FROM `{$table}` LIMIT {$start}, 1000");
            foreach ($result as $row) {
                $row = array_map('addslashes', $row);
                $sql = "INSERT INTO `{$table}` VALUES ('" . str_replace(array("\r", "\n"), array('\\r', '\\n'), implode("', '", $row)) . "');\n";
                if (false === $this->write($sql)) {
                    return false;
                }
            }
            //还有更多数据
            if ($count > $start + 1000) {
                //return array($start + 1000, $count);
                return $this->FileBackupData($table, $start + 1000);
            }
        }
        //备份下一表
        return true;
    }

    /***
     * 导入备份
     */
    public function DbImport($filename) {
        try {
            $db = self::connect();
            $path = realpath($this->config['path']);
            $fileNamePath = $path . '/' . $filename;
            $sql_str = '';
            if ($this->config['compress']) {
                $gz = gzopen($fileNamePath, 'r');
                $buffer_size = 4096; // read 4kb at a time
                while (!gzeof($gz)) {
                    $sql_str .= gzread($gz, $buffer_size);
                }
                gzclose($gz);
                $size = 0;
            } else {
                $size = filesize($fileNamePath);
                $sql_str = file_get_contents($fileNamePath, 'r');
            }
            $sql_arr = explode(';' . PHP_EOL, $sql_str);
            array_pop($sql_arr);
            foreach ($sql_arr as $value) {
                if (!empty($value)) {
                    $db->query($value);
                }
            }
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * 优化表
     * $tables 表名 ，支持数组
     */
    public function DbOptimize($tables = null) {
        if ($tables) {
            $db = self::connect();
            if (is_array($tables)) {
                $tables = implode('`,`', $tables);
                $list = $db->query("OPTIMIZE TABLE `{$tables}`");
            } else {
                $list = $db->query("OPTIMIZE TABLE `{$tables}`");
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 修复表
     * @param String $tables 表名
     * @return String $tables
     */
    public function DbRepair($tables = null) {
        if ($tables) {
            $db = self::connect();
            if (is_array($tables)) {
                $tables = implode('`,`', $tables);
                $list = $db->query("REPAIR TABLE `{$tables}`");
            } else {
                $list = $db->query("REPAIR TABLE `{$tables}`");
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 写入SQL语句
     * @param string $sql 要写入的SQL语句
     * @return boolean     true - 写入成功，false - 写入失败！
     */
    private function write($sql) {
        $size = strlen($sql);
        //由于压缩原因，无法计算出压缩后的长度，这里假设压缩率为50%，
        //一般情况压缩率都会高于50%；
        $size = $this->config['compress'] ? $size / 2 : $size;
        $this->open($size);
        return $this->config['compress'] ? @gzwrite($this->fp, $sql) : @fwrite($this->fp, $sql);
    }

    /**
     * 打开一个卷，用于写入数据
     * @param integer $size 写入数据的大小
     */
    private function open($size) {
        if ($this->fp) {
            $this->size += $size;
            if ($this->size > $this->config['part']) {
                $this->config['compress'] ? @gzclose($this->fp) : @fclose($this->fp);
                $this->fp = null;
                $this->file['part']++;
                $this->Backup_Init();
            }
        } else {
            $backuppath = $this->config['path'];
            $filename = "{$backuppath}{$this->file['name']}-{$this->file['part']}.sql";
            if ($this->config['compress']) {
                $filename = "{$filename}.gz";
                $this->fp = @gzopen($filename, "a{$this->config['level']}");
            } else {
                $this->fp = @fopen($filename, 'a');
            }
            $this->size = filesize($filename) + $size;
        }
    }

    /**
     * 写入初始数据
     * @return boolean true - 写入成功，false - 写入失败
     */
    private function Backup_Init() {
        $sql = "-- -----------------------------\n";
        $sql .= "-- MySQL Data Transfer \n";
        $sql .= "-- \n";
        $sql .= "-- Host     : " . $this->dbconfig['hostname'] . "\n";
        $sql .= "-- Port     : " . $this->dbconfig['hostport'] . "\n";
        $sql .= "-- Database : " . $this->dbconfig['database'] . "\n";
        $sql .= "-- \n";
        $sql .= "-- Part : #{$this->file['part']}\n";
        $sql .= "-- Date : " . date("Y-m-d H:i:s") . "\n";
        $sql .= "-- -----------------------------\n\n";
        $sql .= "SET FOREIGN_KEY_CHECKS = 0;\n\n";
        return $this->write($sql);
    }

    /**
     * 检查目录是否可写
     * @param string $path 目录
     * @return boolean
     */
    protected function checkPath($path) {
        if (is_dir($path)) {
            return true;
        }
        if (mkdir($path, 0755, true)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 析构方法，用于关闭文件资源
     */
    public function __destruct() {
        $this->config['compress'] ? @gzclose($this->fp) : @fclose($this->fp);
    }
}

?>
