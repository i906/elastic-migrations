<?php declare(strict_types=1);

namespace ElasticMigrations;

use Illuminate\Support\Collection;

interface MigrationRepositoryInterface
{
    public function insert(string $fileName, int $batch): bool;

    public function exists(string $fileName): bool;

    public function delete(string $fileName): bool;

    public function getLastBatchNumber(): ?int;

    public function getLastBatch(): Collection;

    public function getAll(): Collection;
}
