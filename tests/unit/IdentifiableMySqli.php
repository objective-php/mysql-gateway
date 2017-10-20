<?php

namespace Test\ObjectivePHP\Gateway\MySql;


class IdentifiableMySqli extends \mysqli
{
    protected $connectionIdentifier;

    /**
     * @return mixed
     */
    public function getConnectionIdentifier()
    {
        return $this->connectionIdentifier;
    }

    /**
     * @param mixed $connectionIdentifier
     * @return IdentifiableMySqli
     */
    public function setConnectionIdentifier($connectionIdentifier)
    {
        $this->connectionIdentifier = $connectionIdentifier;
        return $this;
    }
}