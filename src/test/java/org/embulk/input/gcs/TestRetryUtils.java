package org.embulk.input.gcs;

import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.storage.StorageException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.test.EmbulkTestRuntime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.embulk.input.gcs.GcsFileInputPlugin.CONFIG_MAPPER;
import static org.embulk.input.gcs.GcsFileInputPlugin.CONFIG_MAPPER_FACTORY;
import static org.embulk.input.gcs.RetryUtils.withRetry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRetryUtils
{
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private RetryUtils.DefaultRetryable<Object> mock;

    @Before
    public void setUp()
    {
        mock = new RetryUtils.DefaultRetryable<Object>()
        {
            @Override
            public Object call()
            {
                return null;
            }
        };
    }

    @Test
    public void testRetryable() throws IOException
    {
        // verify that #isRetryable() returns false for below cases:
        // - GoogleJsonResponseException && details.code == 4xx
        assertFalse(mock.isRetryableException(fakeJsonException(400, "fake_400_ex", null)));
        // - TokenResponseException && statusCode == 4xx
        assertFalse(mock.isRetryableException(fakeTokenException(400, "{}")));
        assertFalse(mock.isRetryableException(fakeTokenException(401, "{\"foo\":\"bar\"}")));
        assertFalse(mock.isRetryableException(fakeTokenException(403, "{ \"error_description\": \"Invalid...\"}")));
        // return true
        // - GoogleJsonResponseException && details.code = 5xx
        assertTrue(mock.isRetryableException(fakeJsonException(500, "fake_500_ex", null)));
        // - GoogleJsonResponseException && details == null && content != null
        assertTrue(mock.isRetryableException(fakeJsonExceptionWithoutDetails(400, "fake_400_ex", "this content will make it retry-able")));
        // - TokenResponseException && statusCode = 5xx
        assertTrue(mock.isRetryableException(fakeTokenException(500, "{}")));
        // - TokenResponseException && details.errorDescription contains 'Invalid JWT'
        assertTrue(mock.isRetryableException(fakeTokenException(403, "{ \"error_description\": \"Invalid JWT...\"}")));
    }

    @Test
    public void testWithRetry() throws Exception
    {
        mock = Mockito.spy(mock);
        Exception ex = new StorageException(403, "Fake Exception");
        Mockito.doThrow(ex).doThrow(ex).doReturn(null).when(mock).call();

        Object result = withRetry(params(), mock);
        assertNull(result);
        Mockito.verify(mock, Mockito.times(3)).call();
    }

    @Test
    public void testWithRetryGiveUp()
    {
        final String expectMsg = "Will retry and give up";
        mock = new RetryUtils.DefaultRetryable<Object>()
        {
            @Override
            public Object call()
            {
                throw new IllegalStateException(expectMsg);
            }
        };
        try {
            withRetry(params(), mock);
        }
        catch (RuntimeException e) {
            // root cause -> RetryGiveUpException -> RuntimeException
            Throwable rootCause = e.getCause().getCause();
            assertEquals(expectMsg, rootCause.getMessage());
            assertTrue(rootCause instanceof IllegalStateException);
        }
    }

    private static RetryUtils.Task params()
    {
        return CONFIG_MAPPER.map(CONFIG_MAPPER_FACTORY.newConfigSource().set("initial_retry_interval_millis", 1), RetryUtils.Task.class);
    }

    private static GoogleJsonResponseException fakeJsonException(final int code, final String message, final String content)
    {
        GoogleJsonResponseException.Builder builder = new GoogleJsonResponseException.Builder(code, message, new HttpHeaders());
        builder.setContent(content);
        return new GoogleJsonResponseException(builder, fakeJsonError(code, message));
    }

    private static GoogleJsonResponseException fakeJsonExceptionWithoutDetails(final int code, final String message, final String content)
    {
        GoogleJsonResponseException.Builder builder = new GoogleJsonResponseException.Builder(code, message, new HttpHeaders());
        builder.setContent(content);
        return new GoogleJsonResponseException(builder, null);
    }

    private static GoogleJsonError fakeJsonError(final int code, final String message)
    {
        GoogleJsonError error = new GoogleJsonError();
        error.setCode(code);
        error.setMessage(message);
        return error;
    }

    private static TokenResponseException fakeTokenException(final int code, final String content) throws IOException
    {
        HttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(String method, String url)
            {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute()
                    {
                        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                        response.addHeader("custom_header", "value");
                        response.setStatusCode(code);
                        response.setContentType(Json.MEDIA_TYPE);
                        response.setContent(content);
                        return response;
                    }
                };
            }
        };
        HttpRequest request = transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
        request.setThrowExceptionOnExecuteError(false);
        return TokenResponseException.from(JacksonFactory.getDefaultInstance(), request.execute());
    }
}
