<?php

declare(strict_types=1);

namespace App\Moi\Pipeline\ProcessCsv\Stage;

use App\Moi\BusinessEvent\ErrorLogger;
use App\Moi\BusinessEvent\OfferImportReporter;
use App\Moi\Exception\RowProcessingException;
use App\Moi\Offer\RowExporter;
use App\Moi\Pipeline\ProcessCsv\Payload as ProcessCsvPayload;
use App\Moi\Pipeline\ProcessRow\Payload as ProcessRowPayload;
use Shopping\Services\CopEntities\Entity\MoiStats;
use Shopping\Services\CopMerchantFeedBundle\Processor\MerchantOfferFeedProcessor;
use Throwable;

final class ProcessRow
{
    public function __construct(
        private RowExporter $rowExporter,
        private OfferImportReporter $reporter,
        private MerchantOfferFeedProcessor $feedProcessor,
        private ErrorLogger $errorLogger,
    ) {
    }

    /**
     * @see ProcessCsvCommand
     */
    public function __invoke(ProcessCsvPayload $payload): ProcessCsvPayload
    {
        $merchantId = $payload->getMerchantId();

        /** @var MoiStats $importStats */
        $importStats = $payload->getImportStats();

        try {
            // Read CSV row by row
            foreach ($this->feedProcessor->processFeed(
                delimiter: $payload->getCsvDelimiter(),
                enclosure: $payload->getCsvEnclosure(),
                headersMapping: $payload->getHeadersMapping(),
                skipInvalidRows: true
            ) as $rowProcessingResult) {
                try {
                    if ($rowProcessingResult->isEndOfFile()) {
                        break;
                    }

                    $this->reporter->addProcessed();

                    $this->rowExporter->__invoke(new ProcessRowPayload(
                        $merchantId,
                        $rowProcessingResult,
                        $importStats
                    ));
                } catch (RowProcessingException $error) {
                    $this->errorLogger->addImportErrorLog(
                        $importStats,
                        $merchantId,
                        $rowProcessingResult->getMerchantOffer(),
                        $error->getMessage(),
                        $error->getContext()
                    );
                }
            }

            $payload->setSuccessfullyProcessed(true);
            $this->reporter->reportCriticalImportErrors($payload->getMerchantId());
            $this->errorLogger->saveImportErrorLog();
        } catch (Throwable $exception) {
            // Exception will be thrown in ErrorHandlerStage, after log & statistics are written
            $this->reporter->detectedProcessingException($exception, $merchantId);
        }

        return $payload;
    }
}
