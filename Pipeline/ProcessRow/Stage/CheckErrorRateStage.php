<?php
declare(strict_types=1);

namespace App\Moi\Pipeline\ProcessRow\Stage;

use App\Moi\BusinessEvent\OfferImportReporter;
use App\Moi\Exception\InvalidFeedException;
use App\Moi\Pipeline\ProcessRow\Payload;

/**
 * Business Logic: if CSV has high percentage of invalid rows, we stop the import process
 */
final class CheckErrorRateStage implements StageInterface
{
    private const DEFAULT_BATCH_SIZE = 2000;
    private const DEFAULT_ERROR_THRESHOLD = 90;

    public function __construct(
        private OfferImportReporter $reporter,
        private int $batchSize = self::DEFAULT_BATCH_SIZE,
        private int $errorThreshold = self::DEFAULT_ERROR_THRESHOLD,
    ) {
    }

    /**
     * @see \App\Moi\Offer\RowExporter
     *
     * @throws InvalidFeedException
     */
    public function __invoke(Payload $payload): Payload
    {
        if ($this->isLastRowInBatch() && $this->allowedErrorPercentageExceeded()) {
            throw new InvalidFeedException(
                sprintf(
                    'Invalid merchant feed: threshold of max %d%% validation errors per batch exceeded.',
                    $this->errorThreshold
                )
            );
        }

        return $payload;
    }

    private function allowedErrorPercentageExceeded(): bool
    {
        $errorPercentage = $this->reporter->getFailedRowsCount() / $this->reporter->getProcessedRowsCount() * 100;

        return $errorPercentage > $this->errorThreshold;
    }

    private function isLastRowInBatch(): bool
    {
        return $this->reporter->getProcessedRowsCount() % $this->batchSize === 0;
    }
}
