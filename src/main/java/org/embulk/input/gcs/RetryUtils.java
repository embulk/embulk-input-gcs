package org.embulk.input.gcs;

import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.function.Predicate;

class RetryUtils
{
    interface Task
    {
        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect GCS server if failed.
        int getMaxConnectionRetry();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("300000")
        int getMaximumRetryIntervalMillis();
    }

    private RetryUtils()
    {
    }

    private static final Logger LOG = Exec.getLogger(RetryUtils.class);

    /**
     * A utility predicate to detect status code 4xx of `GoogleJsonResponseException`
     */
    private static final Predicate<GoogleJsonResponseException> API_ERROR_NOT_RETRY_4XX = e -> {
        if (e.getDetails() == null && e.getContent() != null) {
            LOG.warn("Invalid response was returned : {}", e.getContent());
            return true;
        }
        int statusCode = e.getDetails().getCode();
        return statusCode / 100 != 4;
    };

    /**
     * A utility predicate to detect status code 4xx of `TokenResponseException`
     * But will retry 400 "Invalid JWS..."
     */
    private static final Predicate<TokenResponseException> TOKEN_ERROR_NOT_RETRY_4XX = e -> {
        Optional<String> errDesc = Optional.ofNullable(e.getDetails()).map(TokenErrorResponse::getErrorDescription);
        if (errDesc.isPresent()) {
            // Retry: 400 BadRequest "Invalid JWT..."
            // Caused by: com.google.api.client.auth.oauth2.TokenResponseException: 400 Bad Request
            // {
            //   "error" : "invalid_grant",
            //   "error_description" : "Invalid JWT: No valid verifier found for issuer."
            // }
            if (errDesc.get().contains("Invalid JWT")) {
                LOG.warn("Invalid response was returned : {}", errDesc.get());
                return true;
            }
        }
        return e.getStatusCode() / 100 != 4;
    };

    /**
     * A default (abstract) retryable impl, which makes use of above 2 predicates
     * With default behaviors onRetry, etc.
     *
     * @param <T>
     */
    public static abstract class DefaultRetryable<T> implements RetryExecutor.Retryable<T>
    {
        @Override
        public boolean isRetryableException(Exception exception)
        {
            if (exception instanceof GoogleJsonResponseException) {
                return API_ERROR_NOT_RETRY_4XX.test((GoogleJsonResponseException) exception);
            }
            else if (exception instanceof TokenResponseException) {
                return TOKEN_ERROR_NOT_RETRY_4XX.test((TokenResponseException) exception);
            }
            return true;
        }

        @Override
        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
        {
            String message = String.format("GCS GET request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
            if (retryCount % 3 == 0) {
                LOG.warn(message, exception);
            }
            else {
                LOG.warn(message);
            }
        }

        @Override
        public void onGiveup(Exception firstException, Exception lastException)
        {
        }
    }

    /**
     * Return bucket listing op that is ready for {@code withRetry}
     *
     * @param client
     * @param bucket
     * @param prefix
     * @param lastPath
     * @return
     */
    static DefaultRetryable<Page<Blob>> listing(Storage client, String bucket, String prefix, String lastPath)
    {
        return new DefaultRetryable<Page<Blob>>()
        {
            @Override
            public Page<Blob> call()
            {
                return client.list(bucket, Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.pageToken(lastPath));
            }
        };
    }

    /**
     * Return Blob GET op that is ready for {@code withRetry}
     *
     * @param client
     * @param bucket
     * @param key
     * @return
     */
    static DefaultRetryable<Blob> get(Storage client, String bucket, String key)
    {
        return new DefaultRetryable<Blob>()
        {
            @Override
            public Blob call()
            {
                return client.get(bucket, key);
            }
        };
    }

    /**
     * Utility method
     *
     * @param task
     * @param op
     * @param <T>
     * @return
     */
    static <T> T withRetry(Task task, RetryExecutor.Retryable<T> op)
    {
        try {
            return RetryExecutor.retryExecutor()
                    .withInitialRetryWait(task.getInitialRetryIntervalMillis())
                    .withMaxRetryWait(task.getMaximumRetryIntervalMillis())
                    .withRetryLimit(task.getMaxConnectionRetry())
                    .runInterruptible(op);
        }
        catch (RetryExecutor.RetryGiveupException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
