<?php

namespace ObjectivePHP\Gateway\Mysql;

use Aura\SqlQuery\AbstractQuery;
use Aura\SqlQuery\Common\DeleteInterface;
use Aura\SqlQuery\Common\SelectInterface;
use Aura\SqlQuery\Common\UpdateInterface;
use Aura\SqlQuery\Mysql\Insert;
use Aura\SqlQuery\Mysql\Select;
use Aura\SqlQuery\Mysql\Update;
use Aura\SqlQuery\Quoter;
use ObjectivePHP\Gateway\AbstractGateway;
use ObjectivePHP\Gateway\Entity\EntityInterface;
use ObjectivePHP\Gateway\Exception\NoResultException;
use ObjectivePHP\Gateway\Mysql\Exception\ExecuteException;
use ObjectivePHP\Gateway\Mysql\Exception\PrepareException;
use ObjectivePHP\Gateway\ResultSet\Descriptor\ResultSetDescriptorInterface;
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
     * @var Connection[]
     */
    protected $connections;

    /**
     * Add a mysqli connection link
     *
     * @param \mysqli    $connection
     * @param int        $method
     * @param callable[] $filters
     */
    public function addConnection(\mysqli $connection, $method = self::ALL, callable ...$filters)
    {
        $this->connections[$method][] = new Connection($connection, ...$filters);
    }

    /**
     * Returns mysqli connection links
     *
     * @param int $method
     *
     * @return \mysqli[]
     */
    public function getConnections($method = self::ALL)
    {
        $connections = [];

        foreach ($this->connections as $key => $pool) {
            if ($method & $key) {
                /** @var Connection $connection */
                foreach ($pool as $connection) {
                    if ($connection->runFilters()) {
                        $connections[] = $connection->getLink();
                    }
                }
            }
        }

        return $connections;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll(ResultSetDescriptorInterface $descriptor) : ResultSetInterface
    {
        foreach ($this->getConnections(self::FETCH_ALL) as $connection) {
            $query = (new Select(new Quoter("`", "`")))
                ->cols(['*'])
                ->from($descriptor->getCollectionName());

            try {
                return $this->query($query, $connection);
            } catch (\Exception $e) {
                continue;
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function fetchOne($key) : EntityInterface
    {
        $class = $this->getEntityClass() ? $this->getEntityClass() : $this->getDefaultEntityClass();

        /** @var EntityInterface $entity */
        $entity = new $class;

        $collection = $entity->getEntityCollection() != EntityInterface::DEFAULT_ENTITY_COLLECTION
            ? $entity->getEntityCollection()
            : $this->getDefaultEntityCollection();

        foreach ($this->getConnections(self::FETCH_ONE) as $connection) {
            $query = (new Select(new Quoter("`", "`")))
                ->cols(['*'])
                ->from($collection)
                ->where($entity->getEntityIdentifier() . ' = ?', $key);

            try {
                $resultSet = $this->query($query, $connection);
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
    public function persist(EntityInterface ...$entities) : bool
    {
        $result = true;

        foreach ($this->getConnections(self::PERSIST) as $connection) {
            $connection->begin_transaction();
            foreach ($entities as $entity) {
                $colsToRemove = [];

                $collection = $entity->getEntityCollection() != EntityInterface::DEFAULT_ENTITY_COLLECTION
                    ? $entity->getEntityCollection()
                    : $this->getDefaultEntityCollection();

                if ($entity->isNew()) {
                    $query = (new Insert(new Quoter("`", "`")))
                        ->into($collection);

                    if (is_null($entity[$entity->getEntityIdentifier()])) {
                        $colsToRemove[] = $entity->getEntityIdentifier();
                    }
                } else {
                    $query = (new Update(new Quoter("`", "`")))
                        ->table($collection);

                    $colsToRemove[] = $entity->getEntityIdentifier();
                }

                $fields = array_diff($entity->getEntityFields(), $colsToRemove);

                $query->cols($fields);

                foreach ($fields as $field) {
                    $value = $entity[$field];
                    if ($value instanceof \DateTime) {
                        $query->bindValue($field, $value->format('Y-m-d H:i:s'));
                    } else {
                        $query->bindValue($field, $entity[$field]);
                    }
                }

                if (!$entity->isNew()) {
                    $query->where($entity->getEntityIdentifier() . ' = ?', $entity[$entity->getEntityIdentifier()]);
                }

                try {
                    $this->query($query, $connection);
                } catch (\Exception $e) {
                    $connection->rollback();
                    throw $e;
                }

                if ($entity->isNew()) {
                    $entity[$entity->getEntityIdentifier()] = $connection->insert_id;
                }
            }

            $result = $connection->commit();
        }

        return $result;
    }

    /**
     * @param  string|AbstractQuery $query
     * @param \mysqli               $connection
     *
     * @return bool|ResultSetInterface
     */
    public function query($query, \mysqli $connection)
    {
        $rows = null;

        if ($query instanceof AbstractQuery) {
            $sql = $query->getStatement();
            foreach (array_keys($query->getBindValues()) as $name) {
                $sql = preg_replace(sprintf('/:%s/U', $name), '?', $sql);
            }

            $stmt = $connection->prepare($sql);

            if (!$stmt) {
                throw new PrepareException(
                    sprintf('[%s] %s', $connection->sqlstate, $connection->error), $connection->errno
                );
            }

            $type = '';
            foreach ($query->getBindValues() as $value) {
                if (is_bool($value)) {
                    $type .= 'i';
                } else {
                    $type .= 's';
                }
            }

            $stmt->bind_param($type, ...array_values($query->getBindValues()));

            $result = $stmt->execute();

            if (!$result) {
                throw new ExecuteException(
                    sprintf('[%s] %s', $connection->sqlstate, $connection->error), $connection->errno
                );
            }

            if ($query instanceof SelectInterface) {
                $rows = $stmt->get_result()->fetch_all(\MYSQLI_ASSOC);
            }
        } else {
            $result = $connection->query($query);

            if ($result instanceof \mysqli_result) {
                $rows = $result->fetch_all(\MYSQLI_ASSOC);
            }
        }

        if (!is_null($rows)) {
            $result = new ResultSet();
            foreach ($rows as $row) {
                $result->addEntities($this->entityFactory($row));
            }
        }

        return $result;
        /*$result = null;
        if ($this->shouldCache() && $this->loadFromCache($this->getQueryCacheId($query))) {
            return $this->loadFromCache($this->getQueryCacheId($query));
        }

        switch ($link) {
            case self::READ_ONLY:
                $link = $this->readLink;
                break;

            default:
            case self::READ_WRITE:
                $link = $this->link;
                break;
        }

        if (!$link instanceof \PDO) {
            throw new Exception('Selected link is not a PDO link', Exception::INVALID_RESOURCE);
        }

        $this->lastUsedLink = $link;

        $this->preparePagination($query);

        try {
            if ($query instanceof AbstractQuery) {
                $statement = $query->getStatement();
                if ($this->paginateCurrentQuery) {
                    $statement = 'SELECT SQL_CALC_FOUND_ROWS ' . substr($statement, 7);
                }

                $sth = $link->prepare($statement);
                $sth->execute($query->getBindValues());

                if ($query instanceof SelectInterface) {
                    $rows = $sth->fetchAll(\PDO::FETCH_ASSOC);
                }
            } else {
                $rows = $link->query($query);
            }
        } catch (\PDOException $e) {
            throw new Exception(sprintf("SQL Query failed : %s - %s",
                $query, $this->getLastError()), Exception::SQL_ERROR);
        }


        if (isset($rows)) {
            $entities = $this->prepareResultSet($rows);

            if ($this->shouldCache()) {
                $this->storeInCache($this->getQueryCacheId($query), $entities);
            }

            $result = $entities;
        }

        $this->reset();

        return $result;*/
    }

    /**
     * @param SelectInterface|UpdateInterface|DeleteInterface $query
     * @param ResultSetDescriptorInterface                    $descriptor
     */
    protected function decorateQuery($query, ResultSetDescriptorInterface $descriptor)
    {
        if ($query) {

        }

        foreach ($descriptor->getFilters() as $filter)

        foreach ($descriptor->getAggregationRules() as $property => $aggregationType) {
            //$query->
        };
    }

    /**
     * @param $query
     *
     * @return string
     */
    /*protected function getQueryCacheId($query)
    {
        if ($query instanceof AbstractQuery) {
            return md5($query->getStatement() . serialize($query->getBindValues()));
        } else {
            return md5($query);
        }
    }*/

    /**
     * @param null $link
     *
     * @return mixed
     */
    /*public function getLastInsertId($link = null)
    {
        $link = $link ?: $this->lastUsedLink;

        return $link->lastInsertId();
    }*/

    /*public function prepareResultSet($rows): ResultSetInterface
    {
        $entities = $this->paginateCurrentQuery ? new PaginatedEntitySet() : new EntitySet();
        foreach ($rows as $row) {
            $entities[] = $this->entityFactory($row);
        }

        // inject pagination data into EntitySet
        if ($entities instanceof PaginatedEntitySet) {
            $entities->setCurrentPage($this->currentPage);
            $entities->setPerPage($this->perPage ?: $this->defaultPerPage);
            $totalQuery = "SELECT FOUND_ROWS() as total";
            $total = $this->lastUsedLink->query($totalQuery)->fetchColumn(0);
            $entities->setTotal($total);
        }

        return $entities;
    }*/


    /**
     * @param array $columns
     *
     * @return Select
     */
    /*protected function select(array $columns = array('*'))
    {
        $select = new Select(new Quoter("`", "`"));

        $select->cols($columns);

        return $select;
    }*/

    /**
     * @return Insert
     */
    /*protected function insert()
    {
        $insert = new Insert(new Quoter("`", "`"));

        return $insert;
    }*/

    /**
     * @return Update
     */
    /*protected function update()
    {
        $update = new Update(new Quoter("`", "`"));

        return $update;
    }*/
}
