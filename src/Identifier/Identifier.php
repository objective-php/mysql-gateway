<?php

namespace ObjectivePHP\Gateway\MySql\Identifier;

/**
 * Class Identifier
 *
 * @package ObjectivePHP\Gateway\Mysql\Identifier
 */
class Identifier
{
    /**
     * @var string
     */
    protected $field;

    /**
     * @var bool
     */
    protected $isAutoIncrement;

    /**
     * Identifier constructor.
     *
     * @param string $field
     * @param bool   $isAutoIncrement
     */
    public function __construct(string $field = 'id', bool $isAutoIncrement = true)
    {
        $this->field = $field;
        $this->isAutoIncrement = $isAutoIncrement;
    }

    /**
     * Get Field
     *
     * @return string
     */
    public function getField(): string
    {
        return $this->field;
    }

    /**
     * Get isAutoIncrement
     *
     * @return bool
     */
    public function isAutoIncrement(): bool
    {
        return $this->isAutoIncrement;
    }
}
