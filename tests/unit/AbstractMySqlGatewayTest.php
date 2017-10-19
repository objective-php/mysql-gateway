<?php

namespace Test\ObjectivePHP\Gateway\MySql;

use Codeception\Test\Unit;
use ObjectivePHP\Gateway\MySql\Link;
use ObjectivePHP\Gateway\MySql\AbstractMySqlGateway;

class AbstractMySqlGatewayTest extends Unit
{
    use MySqlGatewayTestTrait;

    /**
     * @return AbstractMySqlGateway|__anonymous@305
     */
    public function createAbstractMySqlGateway()
    {
        return new class extends AbstractMySqlGateway
        {

        };
    }

    /**
     * @param $method
     * @param \callable[] ...$filters
     *
     * @dataProvider linkProvider
     */
    public function testRegisterLink($method = AbstractMySqlGateway::ALL, callable ...$filters)
    {
        $link = $this->createMysqlMock();
        $instance = $this->createAbstractMySqlGateway();

        $instance->registerLink($link, $method, ...$filters);
        $instance->registerLink($link, $method, ...$filters);

        $this->assertAttributeEquals(
            [$method => [
                new Link($link, ...$filters),
                new Link($link, ...$filters),
            ]],
            'links',
            $instance
        );
    }

    /**
     * @return array
     */
    public function listMethods()
    {
        return [
            'method AbstractMySqlGateway::FETCH'     => [AbstractMySqlGateway::FETCH],
            'method AbstractMySqlGateway::FETCH_ONE' => [AbstractMySqlGateway::FETCH_ONE],
            'method AbstractMySqlGateway::FETCH_ALL' => [AbstractMySqlGateway::FETCH_ALL],
            'method AbstractMySqlGateway::PERSIST'   => [AbstractMySqlGateway::PERSIST],
            'method AbstractMySqlGateway::UPDATE'    => [AbstractMySqlGateway::UPDATE],
            'method AbstractMySqlGateway::DELETE'    => [AbstractMySqlGateway::DELETE],
            'method AbstractMySqlGateway::PURGE'     => [AbstractMySqlGateway::PURGE],
            'method AbstractMySqlGateway::WRITE'     => [AbstractMySqlGateway::WRITE],
            'method AbstractMySqlGateway::READ'      => [AbstractMySqlGateway::READ],
            'method AbstractMySqlGateway::ALL'       => [AbstractMySqlGateway::ALL],
        ];
    }
    /**
     * @return array
     */
    public function linkProvider()
    {
        $filter = $this->filtersProvider()['trueFilter'][0];

        $linksData = $this->listMethods();

        $linksData['default'] = [];

        // note that it seems no direct validation of any method existence is done.
        $linksData['128 is an invalid value but alas it works !!'] = [128];
        $linksData['a text string, invalid value, works again!!'] = ['whatever compiles!'];

        $linksData['AbstractMySqlGateway::FETCH + filter']  = [AbstractMySqlGateway::FETCH, $filter];
        $linksData['AbstractMySqlGateway::FETCH + filters'] = [AbstractMySqlGateway::FETCH, $filter, $filter, $filter];

        return $linksData;
    }

    /**
     * Target is to "filter" registered Links according to their registration methods and researched method
     *
     * Due to extensive tests on Link class,
     * they will be given either 0 or 1 filter, giving when calling runFilters():
     *  - true  (no filter),
     *  - true  (true return callable),
     *  - false (false return callable)
     *
     * The FILTER part is a sanity check for their proper integration in AbstractMysqlGateway
     *
     * With a Gateway populated with Links, method -> filterResult:
     *  - PERSIST -> true
     *  - FETCH_ONE -> false
     *  - FETCH_ALL -> true (no filter)
     *  - READ -> false
     *  - READ -> true
     *  - READ -> true (no filter)
     *
     * Expected output should be, method -> count :
     *  - PERSIST -> 1
     *  - READ -> 2 (RE
     *  - FETCH_ALL -> 3 (FETCH_ALL , READ *2)
     *  - any other -> 0
     */
    public function getLinksProvider()
    {
        $instance = $this->createAbstractMySqlGateway();

        $linksProviderData = array_map(function($array) use ($instance) {
            $array[] = $instance;
            $array[] = [];
            return $array;
        }, $this->listMethods());

        $trueFilter = function() {
            return true;
        };
        $falseFilter = function() {
            return false;
        };

        $linksData = [
            'PERSIST' => [
                'methodName' => 'method AbstractMySqlGateway::PERSIST',
                'methodId' => AbstractMySqlGateway::PERSIST,
                'filter' => $trueFilter,
            ],
            'FETCH_ONE_false' => [
                'methodName' => 'method AbstractMySqlGateway::FETCH_ONE',
                'methodId' => AbstractMySqlGateway::FETCH_ONE,
                'filter' => $falseFilter,
            ],
            'FETCH_ALL' => [
                'methodName' => 'method AbstractMySqlGateway::FETCH_ALL',
                'methodId' => AbstractMySqlGateway::FETCH_ALL,
            ],
            'READ_true' => [
                'methodName' => 'method AbstractMySqlGateway::READ',
                'methodId' => AbstractMySqlGateway::READ,
                'filter' => $trueFilter,
            ],
            'READ_false' => [
                'methodName' => 'method AbstractMySqlGateway::READ',
                'methodId' => AbstractMySqlGateway::READ,
                'filter' => $falseFilter,
            ],
            'READ' => [
                'methodName' => 'method AbstractMySqlGateway::READ',
                'methodId' => AbstractMySqlGateway::READ,
            ],
        ];

        // preparing mysqli connection according to specs above.
        $mysqlis = [];
        foreach ($linksData as $key => $data) {
            $mysqlis[$key] = $this->createMysqlMock()->setConnectionIdentifier($data['methodName']);
            if (isset($data['filter'])) {
                $instance->registerLink($mysqlis[$key], $data['methodId'], $data['filter']);
            } else {
                $instance->registerLink($mysqlis[$key], $data['methodId']);
            }
        }

        // updating expected elements according to spec above
        $linksProviderData['method AbstractMySqlGateway::PERSIST'][2]   = $this->extractIdentifiers([
            $mysqlis['PERSIST']
        ]);
        $linksProviderData['method AbstractMySqlGateway::FETCH_ALL'][2] = $this->extractIdentifiers([
            $mysqlis['FETCH_ALL'],
            $mysqlis['READ_true'],
            $mysqlis['READ']
        ]);
        $linksProviderData['method AbstractMySqlGateway::READ'][2]      = $this->extractIdentifiers([
            $mysqlis['READ_true'],
            $mysqlis['READ']
        ]);
        $linksProviderData['method AbstractMySqlGateway::FETCH_ONE'][2] = $this->extractIdentifiers([
            $mysqlis['READ_true'], $mysqlis['READ']
        ]);
        $linksProviderData['method AbstractMySqlGateway::FETCH'][2]     = $this->extractIdentifiers([
            $mysqlis['READ_true'],
            $mysqlis['READ']
        ]);

        return $linksProviderData;
    }

    /**
     * It seems some magical cloning happens when dealing with dataProviders,
     * and \mysqli being uncloneable (its mocks && extension altogether),
     * the only 'same' assert can be done on an identifying property
     *
     * @param $method
     * @param AbstractMySqlGateway $instance
     * @param array $foundLinks
     * @dataProvider getLinksProvider
     */
    public function testGetLinks($method = AbstractMySqlGateway::ALL, $instance, array $foundLinks = [])
    {
        $this->assertSame($foundLinks, $this->extractIdentifiers($instance->getLinks($method)));
    }
}
