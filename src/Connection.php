<?php

namespace ObjectivePHP\Gateway\Mysql;

/**
 * Class Link
 *
 * @package ObjectivePHP\Gateway\Mysql
 */
class Connection
{
    /**
     * @var \mysqli
     */
    protected $connection;

    /**
     * @var callable[]
     */
    protected $filters;

    /**
     * Link constructor.
     *
     * @param \mysqli    $link
     * @param callable[] ...$filters
     */
    public function __construct(\mysqli $link, callable ...$filters)
    {
        $this->setConnection($link);
        $this->setFilters($filters);
    }

    /**
     * Get Connection
     *
     * @return \mysqli
     */
    public function getConnection() : \mysqli
    {
        return $this->connection;
    }

    /**
     * Set Connection
     *
     * @param \mysqli $connection
     *
     * @return $this
     */
    public function setConnection(\mysqli $connection)
    {
        $this->connection = $connection;

        return $this;
    }


    /**
     * Get Filters
     *
     * @return callable[]
     */
    public function getFilters() : array
    {
        return $this->filters;
    }

    /**
     * Set Filters
     *
     * @param callable[] $filters
     *
     * @return $this
     */
    public function setFilters(array $filters)
    {
        $this->filters = $filters;

        return $this;
    }

    /**
     * Run filters
     *
     * @return bool
     */
    public function runFilters() : bool
    {
        if (empty($this->filters)) {
            return true;
        }

        foreach ($this->getFilters() as $filter) {
            if (!$filter()) {
                return false;
            }
        }

        return true;
    }
}
