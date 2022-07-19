<?php
declare(strict_types=1);

namespace App\Moi\Pipeline\ProcessRow\Stage;

use App\Moi\Exception\InvalidFeedException;
use App\Moi\Pipeline\ProcessRow\Payload;

/**
 * Business Logic: stop import if CSV is too big (e.g. "ZIP bomb" use cases)
 *
 * @codeCoverageIgnore
 */
final class CheckProcessedFileSizeStage implements StageInterface
{
    public function __construct(private int $maxFileSize)
    {
    }

    /**
     * @see \App\Moi\Offer\RowExporter
     *
     * @throws  InvalidFeedException
     */
    public function __invoke(Payload $payload): Payload
    {
        if ((int) ftell(STDIN) > $this->maxFileSize) {
            throw new InvalidFeedException(
                sprintf(
                    'Max allowed file size of %d bytes exceeded.',
                    $this->maxFileSize
                )
            );
        }

        return $payload;
    }
}
