<?php

declare(strict_types=1);

namespace App\Moi\Pipeline\Common\Stage;

use App\Command\Moi\ProcessCsvCommand;
use App\Moi\Pipeline\Common\Payload\CsvImportConsumerPayload;
use App\Moi\Pipeline\CsvImportConsumer\Payload as ImportPayload;
use Check24\ShellCommandBundle\Utils\Pipe\Pipe;
use Check24\ShellCommandBundle\Utils\Pipe\PipeFactory;
use LogicException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Shell\Commands\CommandInterface;
use Shopping\Services\CopEntities\Entity\MerchantConfig;
use Shopping\Services\CopEntities\Enum\MerchantOfferFeedCompressionType;
use Shopping\Services\CopEntities\Enum\MerchantOfferFeedFormat;
use Shopping\Services\CopMerchantFeedBundle\Factory\WgetExceptionFactory;
use Shopping\Services\CopMerchantFeedBundle\Model\Import\MerchantFeedConfig;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\Stopwatch\Stopwatch;
use Throwable;

#[AutoconfigureTag(
    name: 'monolog.logger',
    attributes: ['channel' => 'moi_specific']
)]
final class ExecutePipeStage
{
    private LoggerInterface $logger;

    public function __construct(
        private WgetExceptionFactory $wgetExceptionFactory,
        private CommandInterface $teeCommand,
        private CommandInterface $wgetCommand,
        private CommandInterface $zgrepCommand,
        private CommandInterface $funzipCommand,
        private CommandInterface $tarExtractCommand,
        private CommandInterface $backupCommand,
        private CommandInterface $removeEmptyLinesCommand,
        private CommandInterface $xml2csvCommand,
        private CommandInterface $processCsvCommand,
        private CommandInterface $processCsvUnmatchedCommand,
        private string $backupStorageTmpPath,
        ?LoggerInterface $logger = null
    ) {
        $this->logger = $logger ?? new NullLogger();
    }

    /**
     * Pipe:
     *
     * @see ProcessCsvCommand, ProcessCsvUnmatchedOffersCommand
     */
    public function __invoke(CsvImportConsumerPayload $payload): CsvImportConsumerPayload
    {
        $pipe = $this->configurePipe($payload);

        $this->logger->info(
            'MOI: started CSV offer import.',
            ['merchantId' => $payload->getMerchantId()]
        );

        try {
            $stopwatch = new Stopwatch();
            $stopwatch->start($payload->getMerchantId(), self::class);
            $pipe->exec();
            $event = $stopwatch->stop($payload->getMerchantId());

            $this->logger->info(
                'MOI: CSV import successfully finished.',
                [
                    'merchantId' => $payload->getMerchantId(),
                    'duration' => round($event->getDuration() / 1000, 2),
                ]
            );

            $payload->setSuccessfullyProcessed(true);
        } catch (Throwable $error) {
            $this->logger->error(
                'MOI: error in "ExecutePipeStage" step.',
                [
                    'merchantId' => $payload->getMerchantId(),
                    'message' => $error->getMessage(),
                    'importStatsId' => $payload->getImportStats()?->getId(),
                    'importStatsClass' => $payload->getImportStats() ? get_class($payload->getImportStats()) : 'null',
                ]
            );

            // in case of wget exception, re-throw the WgetException
            // so the message handler can catch and reschedule a new message
            if ($this->wgetExceptionFactory->canCreate($error->getMessage())) {
                $error = $this->wgetExceptionFactory->create($error->getMessage());
            }

            throw $error;
        }

        return $payload;
    }

    private function configurePipe(CsvImportConsumerPayload $payload): Pipe
    {
        $commands = [
            'fetch' => [],
            'extract' => [],
            'preprocess' => [],
            'process' => [],
        ];

        if (!$feedConfig = $payload->getMerchantConfig()) {
            throw new LogicException('No Merchant Feed Config in payload');
        }

        $isImportPayload = $payload instanceof ImportPayload;
        $isXmlImport = MerchantOfferFeedFormat::XML === $feedConfig->getFeedFormat()->getValue();

        $commands['fetch'][] = $this->buildPipedCommandDefinition($this->wgetCommand);

        $commands['extract'][] = $this->resolveExtractCommandDefinition($payload);

        // backup files for normal import
        if ($isImportPayload) {
            $commands['extract'][] = $this->buildPipedCommandDefinition($this->backupCommand, [0], [
                'path' => sprintf(
                    '%s/%s.%s.gz',
                    rtrim($this->backupStorageTmpPath, '/'),
                    $payload->getMerchantId(),
                    $isXmlImport ? 'xml' : 'csv'
                ),
            ]);
        }

        if ($isXmlImport) {
            $commands['preprocess'][] = $this->buildPipedCommandDefinition($this->xml2csvCommand);
        } else {
            // cleanup not needed after xml2csv
            $commands['preprocess'][] = $this->buildPipedCommandDefinition($this->removeEmptyLinesCommand);
        }

        $commands['process'][] = $this->buildPipedCommandDefinition($isImportPayload ? $this->processCsvCommand : $this->processCsvUnmatchedCommand);

        $pipe = PipeFactory::createPipe(
            $commands,
            $this->logger,
            $this->teeCommand
        );

        $pipe
            ->addParameter('merchantid', $payload->getMerchantId())
            ->addParameter('readTimeout', '30')
            ->addParameter('importStatsId', (string) $payload->getImportStats()?->getId())
            ->addParameter('url', escapeshellarg($payload->getMerchantUrl()))
            ->addParameter('username', escapeshellarg((string) $feedConfig->getFeedUser()))
            ->addParameter('password', escapeshellarg((string) $feedConfig->getFeedPassword()));

        if ($isXmlImport) {
            $pipe->addParameter('xmlEntity', $feedConfig->getFeedXmlEntityName());
            $pipe->addParameter('rowsLimitCount', '0');
        }

        return $pipe;
    }

    private function buildPipedCommandDefinition(CommandInterface $command, array $exitCodes = [0], array $output = []): array
    {
        return [
            'definition' => $command,
            'output' => $output,
            'exitCodes' => $exitCodes,
        ];
    }

    private function resolveExtractCommandDefinition(CsvImportConsumerPayload $payload): array
    {
        $command = null;
        /** @var MerchantConfig $merchantConfig */
        $merchantConfig = $payload->getMerchantConfig();

        if ($merchantConfig->getFeedCompression()) {
            $compression = $merchantConfig->getFeedCompressionType()->getValue();

            // guess by URL extension
            if (MerchantOfferFeedCompressionType::UNKNOWN === $compression) {
                $guessedCompression = MerchantFeedConfig::guessCompressionByFileExtension($payload->getMerchantUrl());
                $compression = match ($guessedCompression) {
                    MerchantFeedConfig::COMPRESSION_TAR_GZ => MerchantOfferFeedCompressionType::TAR_GZ,
                    MerchantFeedConfig::COMPRESSION_ZIP => MerchantOfferFeedCompressionType::ZIP,
                    MerchantFeedConfig::COMPRESSION_GZ => MerchantOfferFeedCompressionType::GZIP,
                    default => MerchantOfferFeedCompressionType::UNKNOWN,
                };
            }

            $command = match ($compression) {
                MerchantOfferFeedCompressionType::TAR_GZ => $this->tarExtractCommand,
                MerchantOfferFeedCompressionType::ZIP => $this->funzipCommand,
                MerchantOfferFeedCompressionType::GZIP => $this->funzipCommand,
                default => null,
            };
        }

        if (!$command) {
            // no file compression - check if http response is gz encoded
            if ($this->hasGzipEncodingHeader($payload->getMerchantUrl())) {
                $command = $this->funzipCommand;
            } else {
                return $this->buildPipedCommandDefinition($this->zgrepCommand, [0, 1, 2]);
            }
        }

        return $this->buildPipedCommandDefinition($command);
    }

    private function hasGzipEncodingHeader(string $fileUrl): bool
    {
        // remove quotation marks, that was added by escapeshellarg() function
        $fileUrl = trim(string: $fileUrl, characters: '\'');
        if (!$urlInfo = parse_url($fileUrl)) {
            throw new LogicException(sprintf('Url "%s "is invalid', $fileUrl));
        }

        // only for https and http
        if (isset($urlInfo['scheme']) && ($urlInfo['scheme'] === 'https' || $urlInfo['scheme'] === 'http')) {
            // By default get_headers uses a GET request to fetch the headers. If you
            // want to send a HEAD request instead, you can do so using a stream context:
            stream_context_set_default(
                options: [
                    'http' => [
                        'method' => 'HEAD',
                        'header' => 'accept-encoding:gzip',
                    ],
                ]
            );

            $headers = get_headers(
                url: $fileUrl,
                associative: true
            );

            // only trust result on 200 response
            if ($headers && preg_match('@200 OK$@', $headers[0])) {
                $headers = array_change_key_case($headers, CASE_LOWER);

                // do not trust generic html pages when HEAD is not supported on download location
                if (!preg_match('@^text/html@', $headers['content-type'] ?? 'text/html')) {
                    return isset($headers['content-encoding']) && $headers['content-encoding'] === 'gzip';
                }
            }
        }

        return false;
    }
}
