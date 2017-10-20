<?php

namespace Test\ObjectivePHP\Gateway\MySql;

/**
 * Class MySqlGatewayTestTrait
 * @package Test\ObjectivePHP\Gateway\MySql
 */
trait MySqlGatewayTestTrait
{
    /**
     * @return \PHPUnit_Framework_MockObject_MockObject
     */
    public function createMysqlMock()
    {
        $mock = $this->getMockBuilder(IdentifiableMySqli::class)
            ->disableOriginalConstructor()
            ->setMethodsExcept(['getConnectionIdentifier', 'setConnectionIdentifier'])
            ->getMock();

        return $mock;
    }

    /**
     * @return callable[][]
     */
    public function filtersProvider() : array
    {
        $trueFilter = function() {
            return true;
        };
        $falseFilter = function() {
            return false;
        };
        return [
            'noFilters' => [
            ],
            'trueFilter' => [
                $trueFilter,
            ],
            'falseFilter' => [
                $falseFilter,
            ],
            'trueFalseFilter' => [
                $trueFilter,
                $falseFilter,
            ],
            'falseTrueFilter' => [
                $falseFilter,
                $trueFilter,
            ],
        ];
    }

    /**
     * @param IdentifiableMySqli[] $mysqlis
     * @return string[]
     */
    public function extractIdentifiers($mysqlis) :array
    {
        return array_map(function($mysqli) {
            /**
             * @var IdentifiableMySqli $mysqli
             */
            return $mysqli->getConnectionIdentifier();
        }, $mysqlis);
    }
}
