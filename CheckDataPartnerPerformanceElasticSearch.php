<?php

namespace Nano\DevOps\Command\Nagios;

use Aws\ImportExport\Exception\InvalidParameterException;
use Elasticsearch\Client;
use Knp\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class CheckDataPartnerPerformanceElasticSearch extends Command
{
    const SEARCH_INDEX_TYPE = 'fluentd';
    const IMPRESSION = 'impression-*';

    private $_client;

    /**
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        parent::__construct();
        $this->_client = $client;
    }

    protected function configure()
    {
        $this
            ->setName("nagios:checkDataPartnerPerformanceElasticSearch")
            ->setDescription("Checks for data partner performance in ES tables")
            ->addOption(Options::INTERVAL_CRITICAL, 'sdic', InputOption::VALUE_REQUIRED, 'critical interval in seconds', null)
            ->addOption(Options::INTERVAL_WARNING, 'stiw', InputOption::VALUE_OPTIONAL, 'warning interval in seconds', null)
            ->addOption(Options::ES_INDEX_NAME,'idn',InputOption::VALUE_REQUIRED, 'index name');
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $staleDateIntervalCritical = $input->getOption(Options::INTERVAL_CRITICAL);
        $this->_validateInterval($staleDateIntervalCritical);
        $staleDateIntervalWarning = $input->getOption(Options::INTERVAL_WARNING);
        $this->_validateInterval($staleDateIntervalWarning);
        $index = $input->getOption(Options::ES_INDEX_NAME);

        $result = $this->_formQueryForSRT($index, $staleDateIntervalCritical);
        $esRawData = $this->_client->search($result);
        $hits = $esRawData['hits']['total'];
        if ($hits == 0) {
            $output->writeln("Critical - Collection " . $index . ".");
            return 2;
        } else {
            if (isset($staleDateIntervalWarning)) {
                $result = $this->_formQueryForSRT($index, $staleDateIntervalWarning);
                $esRawData = $this->_client->search($result);
                $hits = $esRawData['hits']['total'];
                if ($hits == 0) {
                    $output->writeln("Warning - Collection " . $index . ".");
                    return 1;
                } else {
                    $output->writeln('All ok!');
                    return 0;
                }
            }

        }
    }

    /**
     * @param $index
     * @param $time
     * @return array
     */
    protected function _formQueryForSRT($index, $time)
    {

        $params = [
            'index' => $index,
            'type' => self::SEARCH_INDEX_TYPE, // type is fluentd
        ];

        $query = [
            "filtered" => [
                "query" => [
                ],
            "filter" => [
                "bool" => [
                    "must" => [
                        "range" => [
                            "@timestamp" => [
                                'from' => "now-" . $time . "s",
                                'to' => "now"
                            ]
                        ]
                    ]
                ]
            ]
        ]
        ];

            $params['body'] = [
                'size' => 0,
                'query' => $query,
            ];

        return $params;

    }

    protected function _validateInterval($seconds) {
        if (!is_numeric($seconds) || !($seconds > 0))
               {
                   throw new InvalidParameterException('Critical and Warning interval has to be number (seconds) value');
               }
        }
}
