<?php

namespace ObjectivePHP\Gateway\Mysql;

/**
 * Class Link
 *
 * @package ObjectivePHP\Gateway\Mysql
 */
class Link
{
    /**
     * @var \mysqli
     */
    protected $link;

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
        $this->setLink($link);
        $this->setFilters($filters);
    }

    /**
     * Get Link
     *
     * @return \mysqli
     */
    public function getLink() : \mysqli
    {
        return $this->link;
    }

    /**
     * Set Link
     *
     * @param \mysqli $link
     *
     * @return $this
     */
    public function setLink(\mysqli $link)
    {
        $this->link = $link;

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
