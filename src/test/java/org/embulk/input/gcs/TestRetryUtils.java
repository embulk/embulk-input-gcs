/*
 * Copyright 2018 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.input.gcs;

import static org.embulk.input.gcs.GcsFileInputPlugin.CONFIG_MAPPER;
import static org.embulk.input.gcs.GcsFileInputPlugin.CONFIG_MAPPER_FACTORY;
import static org.embulk.input.gcs.RetryUtils.withRetry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import java.io.IOException;
import org.embulk.test.EmbulkTestRuntime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRetryUtils {
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private RetryUtils.DefaultRetryable<Object> mock;

    @Before
    public void setUp() {
        mock = new RetryUtils.DefaultRetryable<Object>() {
            @Override
            public Object call() {
                return null;
            }
        };
    }

    @Test
    public void testRetryable() throws IOException {
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
    public void testWithRetry() throws Exception {
        mock = Mockito.spy(mock);
        Exception ex = new StorageException(403, "Fake Exception");
        Mockito.doThrow(ex).doThrow(ex).doReturn(null).when(mock).call();

        Object result = withRetry(params(), mock);
        assertNull(result);
        Mockito.verify(mock, Mockito.times(3)).call();
    }

    @Test
    public void testWithRetryGiveUp() {
        final String expectMsg = "Will retry and give up";
        mock = new RetryUtils.DefaultRetryable<Object>() {
            @Override
            public Object call() {
                throw new IllegalStateException(expectMsg);
            }
        };
        try {
            withRetry(params(), mock);
        } catch (final RuntimeException e) {
            // root cause -> RetryGiveUpException -> RuntimeException
            Throwable rootCause = e.getCause().getCause();
            assertEquals(expectMsg, rootCause.getMessage());
            assertTrue(rootCause instanceof IllegalStateException);
        }
    }

    private static RetryUtils.Task params() {
        return CONFIG_MAPPER.map(CONFIG_MAPPER_FACTORY.newConfigSource().set("initial_retry_interval_millis", 1), RetryUtils.Task.class);
    }

    private static GoogleJsonResponseException fakeJsonException(final int code, final String message, final String content) {
        GoogleJsonResponseException.Builder builder = new GoogleJsonResponseException.Builder(code, message, new HttpHeaders());
        builder.setContent(content);
        return new GoogleJsonResponseException(builder, fakeJsonError(code, message));
    }

    private static GoogleJsonResponseException fakeJsonExceptionWithoutDetails(final int code, final String message, final String content) {
        GoogleJsonResponseException.Builder builder = new GoogleJsonResponseException.Builder(code, message, new HttpHeaders());
        builder.setContent(content);
        return new GoogleJsonResponseException(builder, null);
    }

    private static GoogleJsonError fakeJsonError(final int code, final String message) {
        GoogleJsonError error = new GoogleJsonError();
        error.setCode(code);
        error.setMessage(message);
        return error;
    }

    private static TokenResponseException fakeTokenException(final int code, final String content) throws IOException {
        HttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(final String method, final String url) {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() {
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
