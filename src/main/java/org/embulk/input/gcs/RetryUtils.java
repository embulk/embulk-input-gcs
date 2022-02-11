package org.embulk.input.gcs;

import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

class RetryUtils
{
    interface Task extends org.embulk.util.config.Task
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

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);

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
    public abstract static class DefaultRetryable<T> implements Retryable<T>
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
    static <T> T withRetry(Task task, Retryable<T> op)
    {
        try {
            return RetryExecutor.builder()
                    .withInitialRetryWaitMillis(task.getInitialRetryIntervalMillis())
                    .withMaxRetryWaitMillis(task.getMaximumRetryIntervalMillis())
                    .withRetryLimit(task.getMaxConnectionRetry())
                    .build()
                    .runInterruptible(op);
        }
        catch (RetryGiveupException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
