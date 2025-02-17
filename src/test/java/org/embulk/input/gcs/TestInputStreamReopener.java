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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.util.file.ResumableInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class TestInputStreamReopener {
    private static class MockReadChannel implements ReadChannel {
        private FileChannel ch;

        MockReadChannel(final FileChannel ch) {
            this.ch = ch;
        }

        @Override
        public boolean isOpen() {
            return this.ch.isOpen();
        }

        @Override
        public void close() {
            try {
                this.ch.close();
            } catch (final IOException ignored) {
                // no-op
            }
        }

        @Override
        public void seek(final long position) throws IOException {
            this.ch.position(position);
        }

        @Override
        public void setChunkSize(final int chunkSize) {
            // no-op
        }

        @Override
        public RestorableState<ReadChannel> capture() {
            return null;
        }

        @Override
        public int read(final ByteBuffer dst) throws IOException {
            return this.ch.read(dst);
        }
    }

    private static final String SAMPLE_PATH = TestInputStreamReopener.class.getResource("/sample_01.csv").getPath();

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testResume() {
        final String bucket = "any_bucket";
        final String key = "any_file";

        final Storage client = mockStorage();

        final SingleFileProvider.InputStreamReopener reopener = new SingleFileProvider.InputStreamReopener(client, bucket, key);
        final byte[] buf = new byte[200];
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final ResumableInputStream ris = new ResumableInputStream(reopener)) {
            int len;
            while ((len = ris.read(buf)) != -1) {
                out.write(buf, 0, len);
            }
            // read from resumable input stream
            String content = out.toString("UTF-8");
            // assert content
            assertString(content);
        } catch (final IOException e) {
            e.printStackTrace();
            fail("Should not throw");
        }
    }

    private Storage mockStorage() {
        Blob blob = Mockito.mock(Blob.class);
        // mock Storage to return ReadChannel
        Storage client = Mockito.mock(Storage.class);
        Mockito.doReturn(blob).when(client).get(eq("any_bucket"), eq("any_file"));
        // to return new instance every time (can't re-use channel, because it'll be closed)
        Mockito.doAnswer(invocation -> new MockReadChannel(mockChannel())).when(blob).reader();
        return client;
    }

    /**
     * Return a mock FileChannel, with simulated error during reads
     */
    private static FileChannel mockChannel() throws IOException {
        FileChannel ch = Mockito.spy(FileChannel.open(Paths.get(SAMPLE_PATH)));
        // success -> error -> success -> error...
        Mockito.doCallRealMethod()
                .doThrow(new IOException("Fake IOException, going to resume"))
                .when(ch).read(any(ByteBuffer.class));
        return ch;
    }

    private static void assertString(final String actual) throws IOException {
        final String expected = Files.asCharSource(new File(SAMPLE_PATH), Charsets.UTF_8).read();
        assertEquals(expected, actual);
    }
}
