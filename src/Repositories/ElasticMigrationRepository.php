<?php declare(strict_types=1);

namespace ElasticMigrations\Repositories;

use ElasticAdapter\Indices\Index;
use ElasticAdapter\Indices\IndexManager;
use ElasticAdapter\Indices\Mapping;
use ElasticMigrations\MigrationRepositoryInterface;
use ElasticMigrations\ReadinessInterface;
use Elasticsearch\Client;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;

final class ElasticMigrationRepository implements ReadinessInterface, MigrationRepositoryInterface
{
    /**
     * @var IndexManager
     */
    private $indexManager;

    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $index;

    public function __construct(IndexManager $indexManager, Client $client)
    {
        $this->index = config('elastic.migrations.table');
        $this->indexManager = $indexManager;
        $this->client = $client;
    }

    public function insert(string $fileName, int $batch): bool
    {
        $params = [
            'index' => $this->index,
            'body' => [
                'migration' => $fileName,
                'batch' => $batch,
            ],
        ];

        $response = $this->client->index($params);

        return Arr::get($response, 'result') === 'created';
    }

    public function exists(string $fileName): bool
    {
        $params = [
            'index' => $this->index,
            'body' => [
                'query' => [
                    'match' => [
                        'migration' => $fileName,
                    ],
                ],
            ],
        ];

        $response = $this->client->count($params);

        return Arr::get($response, 'count', 0) > 0;
    }

    public function delete(string $fileName): bool
    {
        $params = [
            'index' => $this->index,
            'refresh' => true,
            'body' => [
                'query' => [
                    'match' => [
                        'migration' => $fileName,
                    ],
                ],
            ],
        ];

        $response = $this->client->deleteByQuery($params);

        return Arr::get($response, 'deleted', 0) > 0;
    }

    public function getLastBatchNumber(): ?int
    {
        $params = [
            'index' => $this->index,
            'body' => [
                'sort' => [
                    [
                        'batch' => ['order' => 'desc'],
                    ],
                ],
                'size' => 1,
            ],
        ];

        $response = $this->client->search($params);
        $hits = Arr::get($response, 'hits.hits');

        return $hits ? Arr::get($hits, '0._source.batch') : null;
    }

    public function getLastBatch(): Collection
    {
        $params = [
            'index' => $this->index,
            'body' => [
                'aggs' => [
                    'group_by_max_batch' => [
                        'terms' => [
                            'field' => 'batch',
                            'order' => [
                                '_key' => 'desc',
                            ],
                            'size' => 1,
                        ],
                        'aggs' => [
                            'max_batch' => [
                                'top_hits' => [
                                    'from' => 0,
                                ],
                            ],
                        ],
                    ],

                ],
                'size' => 0,
            ],
        ];

        $response = $this->client->search($params);
        $hits = Arr::get($response, 'aggregations.group_by_max_batch.buckets.0.max_batch.hits.hits');

        return collect($hits)
            ->map(static function ($h) {
                return $h['_source'];
            });
    }

    public function getAll(): Collection
    {
        $params = [
            'index' => $this->index,
        ];

        $response = $this->client->search($params);
        $hits = Arr::get($response, 'hits.hits');

        return collect($hits)
            ->map(static function ($h) {
                return $h['_source'];
            });
    }

    public function up(): void
    {
        $mapping = (new Mapping())
            ->text('migration')
            ->integer('batch');

        $index = new Index($this->index, $mapping);

        $this->indexManager->create($index);
    }

    public function down(): void
    {
        $this->indexManager->drop($this->index);
    }

    public function isReady(): bool
    {
        return $this->indexManager->exists($this->index);
    }
}
