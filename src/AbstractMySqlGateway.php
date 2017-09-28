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
use ObjectivePHP\Gateway\AbstractPaginableGateway;
use ObjectivePHP\Gateway\Entity\EntityInterface;
use ObjectivePHP\Gateway\Exception\NoResultException;
use ObjectivePHP\Gateway\MySql\Exception\ExecuteException;
use ObjectivePHP\Gateway\MySql\Exception\MySqlGatewayException;
use ObjectivePHP\Gateway\MySql\Exception\PrepareException;
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
abstract class AbstractMySqlGateway extends AbstractPaginableGateway
{

    /**
     * @var Link[]
     */
    protected $links;

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
     * {@inheritdoc}
     */
    public function fetchOne($key): EntityInterface
    {
        $class = $this->getEntityClass() ? $this->getEntityClass() : $this->getDefaultEntityClass();

        foreach ($this->getLinks(self::FETCH_ONE) as $link) {
            /** @var EntityInterface $entity */
            $entity = new $class;

            $collection = $entity->getEntityCollection() != EntityInterface::DEFAULT_ENTITY_COLLECTION
                ? $entity->getEntityCollection()
                : $this->getDefaultEntityCollection();

            $query = (new Select(new Quoter("`", "`")))
                ->cols(['*'])
                ->from($collection)
                ->where($entity->getEntityIdentifier() . ' = ?', $key);

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
    public function persist(EntityInterface ...$entities): bool
    {
        $result = true;

        $links = $this->getLinks(self::PERSIST);

        if (!$links) throw new MySqlGatewayException('No link found to persist entity');
        foreach ($links as $link) {
            $link->begin_transaction();

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

                // skip cols handled by delegates
                $colsToRemove = array_merge($colsToRemove, array_keys($this->getDelegatePersisters()));

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
                    $this->query($query, $link);
                } catch (\Exception $e) {
                    $link->rollback();
                    throw $e;
                }

                if ($entity->isNew()) {
                    $entity[$entity->getEntityIdentifier()] = $link->insert_id;
                }

                // trigger delegates
                foreach ($this->getDelegatePersisters() as $field => $persister) {
                    $persister($entity[$field], $entity, $this);
                }
            }

            $result = $link->commit();
        }

        return $result;
    }

    /**
     * @param  string|AbstractQuery $query
     * @param \mysqli $link
     *
     * @return bool|ResultSetInterface
     */
    public function query($query, \mysqli $link)
    {
        $rows = null;
        $result = false;

        if ($query instanceof AbstractQuery) {

            $sql = $query->getStatement();
            foreach (array_keys($query->getBindValues()) as $name) {
                $sql = preg_replace(sprintf('/:%s/U', $name), '?', $sql);
            }

            $stmt = $link->prepare($sql);

            if (!$stmt) {
                throw new PrepareException(sprintf('[%s] %s (%s)', $link->sqlstate, $link->error, (string) $sql), $link->errno);
            }

            $boundValues = $query->getBindValues();
            if ($boundValues) {
                $types = '';
                foreach ($query->getBindValues() as $value) {
                    if (is_bool($value)) {
                        $types .= 'i';
                    } elseif (is_int($value)) {
                        $types .= 'i';
                    } elseif (is_float($value)) {
                        $types .= 'd';
                    } else {
                        $types .= 's';
                    }
                }

                $stmt->bind_param($types, ...array_values($boundValues));
            }

            $result = $stmt->execute();

            if (!$result) {
                throw new ExecuteException(sprintf('[%s] %s (%s)', $link->sqlstate, $link->error, (string) $query), $link->errno);
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

    public function fetch(ResultSetDescriptorInterface $resultSetDescriptor): ProjectionInterface
    {
        throw new MySqlGatewayException(sprintf('Method ' . __METHOD__ . ' is not implemented on this gateway'));
    }

    /**
     * {@inheritdoc}
     */
    public function delete(EntityInterface ...$entities)
    {
        $result = true;

        $links = $this->getLinks(self::PERSIST);

        if (!$links) throw new MySqlGatewayException('No link found to delete entity');
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

        foreach($data as $key => $value) {
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
     * @param Select $query
     * @param ResultSetDescriptorInterface $resultSetDescriptor
     * @return QueryInterface
     */
    protected function hydrateQuery(QueryInterface $query, ResultSetDescriptorInterface $resultSetDescriptor): QueryInterface
    {
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
            $property = strpos($filter['property'], '.') === false ?  $resultSetDescriptor->getCollectionName() . '.' . $filter['property'] : $filter['property'];
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
        foreach($resultSetDescriptor->getSort() as $property => $direction)
        {
            if(strpos($property, '.') === false) $property = $resultSetDescriptor->getCollectionName() . '.' . $property;
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

        /** @var Document $document */
        foreach ($result->fetch_assoc() as $data) {
            $entity = $this->entityFactory($data);
            $resultSet[] = $entity;
        }

        return $resultSet;
    }
}
