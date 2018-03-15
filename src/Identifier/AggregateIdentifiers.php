<?php

namespace ObjectivePHP\Gateway\MySql\Identifier;

use ObjectivePHP\Gateway\MySql\Exception\AggregateIdentifiersException;
use ObjectivePHP\Primitives\Collection\Collection;

/**
 * Class CompositeIdentifier
 *
 * @package ObjectivePHP\Gateway\Mysql\Identifier
 */
class AggregateIdentifiers extends Collection
{
    /**
     * @var array
     */
    protected $value = [];

    /**
     * @var bool
     */
    protected $hasAutoIncrement = false;

    /**
     * {@inheritdoc}
     */
    public function __construct(array $input = [])
    {
        $this->restrictTo(Identifier::class);

        parent::__construct($input);
    }

    /**
     * {@inheritdoc}
     *
     * @param mixed      $key
     * @param Identifier $value
     *
     * @return $this
     */
    public function set($key, $value)
    {
        if ($this->hasAutoIncrement() && $value->isAutoIncrement()) {
            throw new AggregateIdentifiersException(
                'Can\'t set the auto-increment identifier as an auto-increment identifier was already set'
            );
        }

        $this->hasAutoIncrement = $value->isAutoIncrement();

        return parent::set($key, $value);
    }

    /**
     * Get isAutoIncremented
     *
     * @return bool
     */
    public function hasAutoIncrement(): bool
    {
        return $this->hasAutoIncrement;
    }
}
