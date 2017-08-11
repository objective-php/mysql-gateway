<?php
/**
 * Created by PhpStorm.
 * User: gde
 * Date: 10/08/2017
 * Time: 11:13
 */

namespace ObjectivePHP\Gateway\MySql\Config;


use ObjectivePHP\Config\SingleValueDirectiveGroup;

class MySqlConnection extends SingleValueDirectiveGroup
{
    public function __construct($identifier, $host, $username = null, $password = null, $database = null, $port = null)
    {
        $value = compact('host', 'username', 'password', 'database', 'port');

        parent::__construct($identifier, $value);
    }


    /**
     * @return mixed
     */
    public function getHost()
    {
        return $this->value['host'];
    }

    /**
     * @param mixed $host
     */
    public function setHost($host)
    {
        $this->value['host'] = $host;
    }

    /**
     * @return mixed
     */
    public function getPort()
    {
        return $this->value['port'];
    }

    /**
     * @param mixed $port
     */
    public function setPort($port)
    {
        $this->value['port'] = $port;
    }

    /**
     * @return mixed
     */
    public function getUsername()
    {
        return $this->value['username'];
    }

    /**
     * @param mixed $username
     */
    public function setUsername($username)
    {
        $this->value['username'] = $username;
    }

    /**
     * @return mixed
     */
    public function getPassword()
    {
        return $this->value['password'];
    }

    /**
     * @param mixed $password
     */
    public function setPassword($password)
    {
        $this->value['password'] = $password;
    }

    /**
     * @return mixed
     */
    public function getDatabase()
    {
        return $this->value['database'];
    }

    /**
     * @param mixed $database
     */
    public function setDatabase($database)
    {
        $this->value['database'] = $database;
    }

}