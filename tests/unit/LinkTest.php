<?php

namespace Test\ObjectivePHP\Gateway\MySql;

use Codeception\Test\Unit;
use ObjectivePHP\Gateway\MySql\Link;

class LinkTest extends Unit
{
    use MySqlGatewayTestTrait;

    /**
     * @param callable[] ...$filters
     * @dataProvider filtersProvider
     */
    public function testConstruct(callable ...$filters)
    {
        $mysqli = $this->createMysqlMock();

        $link = new Link($mysqli, ...$filters);
        $this->assertSame($mysqli, $link->getLink());
        $this->assertSame($filters, $link->getFilters());
    }

    /**
     * @param callable[] ...$filters
     * @dataProvider filtersProvider
     */
    public function testGettersSetters(callable ...$filters)
    {
        $link = new Link($this->createMysqlMock());

        $anotherMysqli = $this->createMysqlMock();
        $this->assertSame($anotherMysqli, $link->setLink($anotherMysqli)->getLink());
        $this->assertEquals([], $link->getFilters());

        $link->setFilters($filters);
        $this->assertSame($filters, $link->getFilters());

    }

    /**
     * @param callable[] $filters
     * @param $expectedValue
     * @dataProvider filtersRunResultsProvider
     */
    public function testRunFilters($filters, $expectedValue)
    {
        $this->assertEquals($expectedValue, (new Link($this->createMysqlMock(), ...$filters))->runFilters());
    }

    public function filtersRunResultsProvider()
    {
        $filters = $this->filtersProvider();
        $expected = [
            'noFilters'       => [$filters['noFilters'], true],
            'trueFilter'      => [$filters['trueFilter'], true],
            'falseFilter'     => [$filters['falseFilter'], false],
            'trueFalseFilter' => [$filters['trueFalseFilter'], false],
            'falseTrueFilter' => [$filters['falseTrueFilter'], false],
        ];
        return $expected;
    }
}
