<?php

namespace Shopping\Services\CopMerchantFeedBundle\Factory;

use RuntimeException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetAuthenticationFailureException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetFileIOErrorException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetGenericErrorException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetNetworkFailureException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetParseErrorException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetProtocolErrorsException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetServerErrorException;
use Shopping\Services\CopMerchantFeedBundle\Exception\Wget\WgetSSLVerificationFailureException;

class WgetExceptionFactory
{
    private const WGET_MESSAGE_EXCEPTION_PATTERN = '/.+\\/(wget\\s).*(Exit-Code: \\d).*/';
    private const WGET_EXIT_CODE_PATTERN = '/Exit-Code: \d/';

    /**
     * @see https://man7.org/linux/man-pages/man1/wget.1.html
     * @see https://www.gnu.org/software/wget/manual/html_node/Exit-Status.html
     */
    private const WGET_EXIT_STATUS_GENERIC_ERROR = 1;
    private const WGET_EXIT_STATUS_PARSE_ERROR = 2;
    private const WGET_EXIT_STATUS_INPUT_OUTPUT_ERROR = 3;
    private const WGET_EXIT_STATUS_NETWORK_FAILED = 4;
    private const WGET_EXIT_STATUS_SSL_VERIFICATION_FAILED = 5;
    private const WGET_EXIT_STATUS_AUTHENTICATION_FAILED = 6;
    private const WGET_EXIT_STATUS_PROTOCOL_ERRORS = 7;
    private const WGET_EXIT_STATUS_SERVER_ERROR_RESPONSE = 8;

    public function canCreate(string $message): bool
    {
        return preg_match(self::WGET_MESSAGE_EXCEPTION_PATTERN, $message) === 1;
    }

    public function create(string $message): WgetException
    {
        $exitCode = $this->getWgetExitCode($message);

        switch ($exitCode) {
            case self::WGET_EXIT_STATUS_GENERIC_ERROR:
                return new WgetGenericErrorException();

            case self::WGET_EXIT_STATUS_PARSE_ERROR:
                return new WgetParseErrorException();

            case self::WGET_EXIT_STATUS_INPUT_OUTPUT_ERROR:
                return new WgetFileIOErrorException();

            case self::WGET_EXIT_STATUS_NETWORK_FAILED:
                return new WgetNetworkFailureException();

            case self::WGET_EXIT_STATUS_SSL_VERIFICATION_FAILED:
                return new WgetSSLVerificationFailureException();

            case self::WGET_EXIT_STATUS_AUTHENTICATION_FAILED:
                return new WgetAuthenticationFailureException();

            case self::WGET_EXIT_STATUS_PROTOCOL_ERRORS:
                return new WgetProtocolErrorsException();

            case self::WGET_EXIT_STATUS_SERVER_ERROR_RESPONSE:
                return new WgetServerErrorException();
        }

        throw new RuntimeException(sprintf('Undefined wget exit code: %d', $exitCode));
    }

    private function getWgetExitCode(string $message): int
    {
        preg_match(self::WGET_EXIT_CODE_PATTERN, $message, $matches);

        return (int) substr($matches[0], -1);
    }
}
