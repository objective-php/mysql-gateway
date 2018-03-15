<?php

namespace ObjectivePHP\Gateway\MySql;

use Aura\SqlQuery\AbstractQuery;
use Aura\SqlQuery\Mysql\Delete;
use Aura\SqlQuery\Common\SelectInterface;
use Aura\SqlQuery\Mysql\Insert;
use Aura\SqlQuery\Mysql\Select;
use Aura\SqlQuery\Mysql\Update;
use Aura\SqlQuery\QueryInterface;
use Aura\SqlQuery\Quoter;
use ObjectivePHP\Gateway\AbstractGateway;
use ObjectivePHP\Gateway\Exception\GatewayException;
use ObjectivePHP\Gateway\Exception\NoResultException;
use ObjectivePHP\Gateway\GatewayInterface;
use ObjectivePHP\Gateway\Model\Relation\HasMany;
use ObjectivePHP\Gateway\Model\Relation\HasOne;
use ObjectivePHP\Gateway\Model\Relation\Relation;
use ObjectivePHP\Gateway\MySql\Exception\ExecuteException;
use ObjectivePHP\Gateway\MySql\Exception\MySqlGatewayException;
use ObjectivePHP\Gateway\MySql\Exception\PrepareException;
use ObjectivePHP\Gateway\MySql\Identifier\AggregateIdentifiers;
use ObjectivePHP\Gateway\MySql\Identifier\Identifier;
use ObjectivePHP\Gateway\Projection\ProjectionInterface;
use ObjectivePHP\Gateway\ResultSet\Descriptor\ResultSetDescriptorInterface;
use ObjectivePHP\Gateway\ResultSet\PaginatedResultSet;
use ObjectivePHP\Gateway\ResultSet\PaginatedResultSetInterface;
use ObjectivePHP\Gateway\ResultSet\ResultSet;
use ObjectivePHP\Gateway\ResultSet\ResultSetInterface;

/**
 * Class AbstractMySqlGateway
 *
 * @package Fei\ApiServer\Gateway
 */
abstract class AbstractMySqlGateway extends AbstractGateway
{
    /**
     * @var Link[]
     */
    protected $links;

    /**
     * @var string
     */
    protected $relation;

    /**
     * @var AggregateIdentifiers
     */
    protected $identifiers;

    /**
     * @var \mysqli
     */
    protected $currentParentLink;

    /**
     * @var array;
     */
    protected $persistedEntity = [];

    /**
     * AbstractMySqlGateway constructor.
     *
     * @param string|null $entityClass
     * @param string|null $relation
     */
    public function __construct(string $entityClass = null, string $relation = null)
    {
        if (!is_null($relation)) {
            $this->setRelation($relation);
        }

        $this->setIdentifiers(new AggregateIdentifiers());

        parent::__construct($entityClass);

        if (!$this->getIdentifiers()->count()) {
            $this->registerIdentifiers(new Identifier());
        }
    }

    /**
     * Add a mysqli connection link
     *
     * @param \mysqli $link
     * @param int $method
     * @param callable[] $filters
     */
    public function registerLink(\mysqli $link, $method = self::ALL, callable ...$filters)
    {
        $this->links[$method][] = new Link($link, ...$filters);
    }

    /**
     * Returns mysqli connection links
     *
     * @param int $method
     *
     * @return \mysqli[]
     */
    public function getLinks($method = self::ALL)
    {
        $links = [];

        foreach ($this->links as $key => $pool) {
            if ($method & $key) {
                /** @var Link $link */
                foreach ($pool as $link) {
                    if ($link->runFilters()) {
                        $links[] = $link->getLink();
                    }
                }
            }
        }

        return $links;
    }

    /**
     * Get Relation
     *
     * @return string
     */
    public function getRelation(): string
    {
        return $this->relation;
    }

    /**
     * Set Relation
     *
     * @param string $relation
     *
     * @return $this
     */
    public function setRelation(string $relation)
    {
        $this->relation = $relation;

        return $this;
    }

    /**
     * Get Identifier
     *
     * @return AggregateIdentifiers
     */
    public function getIdentifiers(): AggregateIdentifiers
    {
        return $this->identifiers;
    }

    /**
     * Set Identifiers
     *
     * @param AggregateIdentifiers $identifiers
     */
    public function setIdentifiers(AggregateIdentifiers $identifiers)
    {
        $this->identifiers = $identifiers;
    }

    /**
     * Register an identifier
     *
     * @param Identifier $identifier
     *
     * @return $this
     */
    public function registerIdentifiers(Identifier $identifier)
    {
        $this->identifiers[] = $identifier;

        return $this;
    }

    /**
     * Get ParentLink
     *
     * @return \mysqli|null
     */
    public function getCurrentParentLink()
    {
        return $this->currentParentLink;
    }

    /**
     * Set ParentLink
     *
     * @param \mysqli $currentParentLink
     *
     * @return $this
     */
    public function setCurrentParentLink(\mysqli $currentParentLink)
    {
        $this->currentParentLink = $currentParentLink;

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchOne($key)
    {
        $class = $this->getEntityClass();

        foreach ($this->getLinks(self::FETCH_ONE) as $link) {
            $entity = new $class;

            $collection = $this->getRelation();

            $query = (new Select(new Quoter("`", "`")))
                ->cols(['*'])
                ->from($collection)
                ->where($this->getIdentifiers()[0] . ' = ?', $key); // FIXME Identifier is an array here

            try {
                $resultSet = $this->query($query, $link);
            } catch (\Exception $e) {
                continue;
            }

            if ($resultSet instanceof ResultSetInterface && $resultSet->count()) {
                return array_pop($resultSet->toArray());
            }
        }

        throw new NoResultException(sprintf('Unable to find an entity of type "%s" for identifier "%s"', $class, $key));
    }

    /**
     * {@inheritdoc}
     */
    public function persist(...$entities)
    {
        $links = $this->getLinks(self::PERSIST);

        if (!$links) {
            throw new MySqlGatewayException('No link found to persist entity');
        }

        foreach ($entities as $entity) {
            if ($this->isPersisted($entity)) {
                continue;
            }

            $data = $this->getHydrator()->extract($entity);

            $colsToRemove = [];

            $relation = $this->getRelation();

            $isNew = $this->isNew($data);

            if ($isNew) {
                $query = (new Insert(new Quoter("`", "`")))
                    ->into($relation);

                /** @var Identifier $identifier */
                foreach ($this->getIdentifiers() as $identifier) {
                    if (array_key_exists($identifier->getField(), $data)
                        && is_null($data[$identifier->getField()])
                    ) {
                        $colsToRemove[] = $identifier->getField();
                    }
                }
            } else {
                $query = (new Update(new Quoter("`", "`")))
                    ->table($relation);

                /** @var Identifier $identifier */
                foreach ($this->getIdentifiers() as $identifier) {
                    $colsToRemove[] = $identifier->getField();
                    $query->where($identifier->getField() . ' = ?', $data[$identifier->getField()]);
                }
            }

            $colsToRemove = array_merge($colsToRemove, array_keys($this->getRelatedFields(HasMany::class)));

            $fields = array_diff(array_keys($data), $colsToRemove);

            /**
             * @var string $field
             * @var Relation $relation
             */
            foreach ($this->getRelatedFields(HasOne::class) as $field => $relation) {
                $relatedEntityClass = $relation->getEntityClass();

                if (array_key_exists($field, $data) && $data[$field] instanceof $relatedEntityClass) {
                    /** @var GatewayInterface $gateway */
                    $gateway = $this->getGatewaysFactory()->get($relation->getEntityClass());

                    if ($gateway instanceof AbstractMySqlGateway) {
                        $dataRelated = $gateway->getHydrator()->extract($data[$field]);

                        /** @var Identifier $identifier */
                        foreach ($gateway->getIdentifiers() as $identifier) {
                            $data[$gateway->relation . '_' . $identifier->getField()]
                                = $dataRelated[$identifier->getField()];
                        }
                    }
                }

                $fields = array_diff(array_keys($data), [$field]);
            }

            $query->cols($fields);

            foreach ($fields as $field) {
                $value = $data[$field];
                if ($value instanceof \DateTime) {
                    $query->bindValue($field, $value->format('Y-m-d H:i:s'));
                } else {
                    $query->bindValue($field, $value);
                }
            }

            foreach ($links as $link) {
                $mustCommit = true;

                if ($this->getCurrentParentLink() !== $link) {
                    $link->autocommit(false);
                    $link->begin_transaction();

                    $mustCommit = false;
                }

                try {
                    $this->query($query, $link);
                } catch (\Exception $e) {
                    $link->rollback();
                    throw $e;
                }

                if ($isNew) {
                    /** @var Identifier $identifier */
                    foreach ($this->getIdentifiers() as $identifier) {
                        if ($identifier->isAutoIncrement()) {
                            $data[$identifier->getField()] = $link->insert_id;
                        }
                    }
                }

                $this->getHydrator()->hydrate($data, $entity);

                $this->markAsPersisted($entity);

                /**
                 * @var string $field
                 * @var Relation $relation
                 */
                foreach ($this->getRelatedFields() as $field => $relation) {
                    /** @var GatewayInterface $gateway */
                    $gateway = $this->getGatewaysFactory()->get($relation->getEntityClass());

                    if ($gateway instanceof AbstractMySqlGateway) {
                        $gateway->setCurrentParentLink($link);
                    }

                    if ($relation instanceof HasMany) {
                        $gateway->persist(...$data[$field]);
                    } elseif ($relation instanceof HasOne) {
                        $gateway->persist($data[$field]);
                    }
                }

                if ($mustCommit) {
                    $link->commit();
                }
            }
        }
    }

    /**
     * Perform a query
     *
     * @param  string|AbstractQuery $query
     * @param \mysqli               $link
     *
     * @return bool|ResultSetInterface
     *
     * @throws ExecuteException
     * @throws PrepareException
     * @throws GatewayException
     */
    public function query($query, \mysqli $link)
    {
        $rows = null;
        $result = false;

        if ($query instanceof AbstractQuery) {
            $sql = $query->getStatement();
            foreach (array_keys($query->getBindValues()) as $name) {
                $sql = preg_replace(sprintf('/:%s/U', $name), '?', $sql, 1);
            }

            $stmt = $link->prepare($sql);

            if (!$stmt) {
                throw new PrepareException(
                    sprintf('[%s] %s (%s)', $link->sqlstate, $link->error, (string) $sql),
                    $link->errno
                );
            }

            $boundValues = $query->getBindValues();
            if ($boundValues) {
                $types = '';
                foreach ($query->getBindValues() as $value) {
                    if (is_bool($value)) {
                        $types .= 'i';
                    } else {
                        $types .= 's';
                    }
                }

                $stmt->bind_param($types, ...array_values($boundValues));
            }

            $result = $stmt->execute();

            if (!$result) {
                throw new ExecuteException(
                    sprintf('[%s] %s (%s)', $link->sqlstate, $link->error, (string) $query),
                    $link->errno
                );
            }

            if ($query instanceof SelectInterface) {
                $rows = $stmt->get_result()->fetch_all(\MYSQLI_ASSOC);
                $result = new ResultSet();
                foreach ($rows as $row) {
                    $result->addEntities($this->entityFactory($row));
                }
            }

        } else {
            $data = $link->query($query);

            if ($data instanceof \mysqli_result) {
                $result = new ResultSet();
                while ($row = $data->fetch_assoc()) {
                    $result->addEntities($this->entityFactory($row));
                }
            }
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    public function fetch(ResultSetDescriptorInterface $resultSetDescriptor): ProjectionInterface
    {
        throw new MySqlGatewayException(sprintf('Method ' . __METHOD__ . ' is not implemented on this gateway'));
    }

    /**
     * {@inheritdoc}
     */
    public function delete(...$entities)
    {
        $result = true;

        $links = $this->getLinks(self::DELETE);

        if (!$links) {
            throw new MySqlGatewayException('No link found to delete entity');
        }

        foreach ($links as $link) {
            $link->begin_transaction();

            foreach ($entities as $entity) {
                $collection = $entity->getEntityCollection() != EntityInterface::DEFAULT_ENTITY_COLLECTION
                    ? $entity->getEntityCollection()
                    : $this->getDefaultEntityCollection();

                $query = (new Delete(new Quoter("`", "`")))->from($collection);

                $query->where($entity->getEntityIdentifier() . '=:id')
                    ->bindValue('id', $entity[$entity->getEntityIdentifier()]);

                try {
                    $this->query($query, $link);
                } catch (\Exception $e) {
                    $link->rollback();
                }
            }

            $result = $link->commit();
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    public function purge(ResultSetDescriptorInterface $descriptor)
    {
        throw new MySqlGatewayException(sprintf('Method ' . __METHOD__ . ' is not implemented on this gateway'));
    }

    /**
     * @inheritdoc
     */
    public function update(ResultSetDescriptorInterface $descriptor, $data)
    {
        $links = $this->getLinks(self::UPDATE);
        $query = new Update(new Quoter('`', '`'));

        foreach ($data as $key => $value) {
            $query->set($key, ':value_' . $key);
            $query->bindValue('value_' . $key, $value);
        }

        $this->hydrateQuery($query, $descriptor);
        $result = $this->query($query, array_pop($links));

        return $result;
    }

    /**
     * @inheritdoc
     */
    public function fetchAll(ResultSetDescriptorInterface $descriptor): ResultSetInterface
    {
        $links = $this->getLinks(self::READ);
        $query = new Select(new Quoter('`', '`'));
        $this->hydrateQuery($query, $descriptor);

        $result = $this->query($query, array_pop($links));

        return $result;
    }

    /**
     * @param Select|QueryInterface        $query
     * @param ResultSetDescriptorInterface $resultSetDescriptor
     *
     * @return QueryInterface
     */
    protected function hydrateQuery(
        QueryInterface $query,
        ResultSetDescriptorInterface $resultSetDescriptor
    ): QueryInterface {
        $quoter = new Quoter('`', '`');

        if ($query instanceof Update) {
            $query->table($resultSetDescriptor->getCollectionName());
        } else {
            $query->from($resultSetDescriptor->getCollectionName());
            $query->cols(['*']);
        }

        foreach ($resultSetDescriptor->getFilters() as $filter) {
            $operator = $filter['operator'];
            $paramId = uniqid('param_');
            $property = strpos($filter['property'], '.') === false
                ? $resultSetDescriptor->getCollectionName() . '.' . $filter['property']
                : $filter['property'];
            $query->where($quoter->quoteName($property) . ' ' . $operator . ' :' . $paramId);
            $query->bindValue($paramId, $filter['value']);
        }

        $size = 0;
        if ($size = $resultSetDescriptor->getSize()) {
            $query->limit($size);
        }

        if ($page = $resultSetDescriptor->getPage()) {
            $size = $resultSetDescriptor->getPageSize();
            $query->limit($size);
            $query->offset(($page - 1) * $size);
        }

        $orderBy = [];
        foreach ($resultSetDescriptor->getSort() as $property => $direction) {
            if (strpos($property, '.') === false) {
                $property = $resultSetDescriptor->getCollectionName() . '.' . $property;
            }

            $orderBy[] = $property . ' ' . $direction;
        }

        $query->orderBy($orderBy);

        return $query;
    }

    protected function buildResultSet(\mysqli_result $result): ResultSetInterface
    {
        $resultSet = ($this->paginateNextQuery) ? new PaginatedResultSet() : new ResultSet();

        if ($resultSet instanceof PaginatedResultSetInterface) {
            $resultSet->setCurrentPage($this->currentPage)->setPerPage($this->pageSize)->setTotal(
                count($result)
            );
        }

        foreach ($result->fetch_assoc() as $data) {
            $entity = $this->entityFactory($data);
            $resultSet[] = $entity;
        }

        return $resultSet;
    }

    /**
     * Tells if an entity is persisted or not
     *
     * @param array $data
     *
     * @return bool
     *
     * @throws MySqlGatewayException If identifier is not consistent
     */
    protected function isNew(array $data): bool
    {
        $isNew = true;
        $hasNullIdentifier = false;

        /** @var Identifier $identifier */
        foreach ($this->getIdentifiers() as $identifier) {
            if (array_key_exists($identifier->getField(), $data) && !is_null($data[$identifier->getField()])) {
                if ($hasNullIdentifier) {
                    throw new MySqlGatewayException('The identifiers form composite identifier must all be null');
                }

                $isNew = false;
            } else {
                $hasNullIdentifier = true;
            }
        }

        return $isNew;
    }

    /**
     * Tells if an entity is already persisted
     *
     * @param object $entity
     *
     * @return bool
     */
    protected function isPersisted($entity): bool
    {
        return in_array($entity, $this->persistedEntity);
    }

    /**
     * Mark an entity as persisted
     *
     * @param $entity
     */
    protected function markAsPersisted($entity)
    {
        if (!$this->isPersisted($entity)) {
            $this->persistedEntity[] = clone $entity;
        }
    }
}
