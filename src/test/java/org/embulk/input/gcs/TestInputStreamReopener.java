package org.embulk.input.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.embulk.EmbulkTestRuntime;
import org.embulk.util.file.ResumableInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class TestInputStreamReopener
{
    private static class MockReadChannel implements ReadChannel
    {
        private FileChannel ch;

        MockReadChannel(FileChannel ch)
        {
            this.ch = ch;
        }

        @Override
        public boolean isOpen()
        {
            return this.ch.isOpen();
        }

        @Override
        public void close()
        {
            try {
                this.ch.close();
            }
            catch (IOException ignored) {
            }
        }

        @Override
        public void seek(long position) throws IOException
        {
            this.ch.position(position);
        }

        @Override
        public void setChunkSize(int chunkSize)
        {
            // no-op
        }

        @Override
        public RestorableState<ReadChannel> capture()
        {
            return null;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException
        {
            return this.ch.read(dst);
        }
    }

    private static final String SAMPLE_PATH = TestInputStreamReopener.class.getResource("/sample_01.csv").getPath();

    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testResume()
    {
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
        }
        catch (IOException e) {
            e.printStackTrace();
            fail("Should not throw");
        }
    }

    private Storage mockStorage()
    {
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
     *
     * @return
     * @throws IOException
     */
    private static FileChannel mockChannel() throws IOException
    {
        FileChannel ch = Mockito.spy(FileChannel.open(Paths.get(SAMPLE_PATH)));
        // success -> error -> success -> error...
        Mockito.doCallRealMethod()
                .doThrow(new IOException("Fake IOException, going to resume"))
                .when(ch).read(any(ByteBuffer.class));
        return ch;
    }

    private static void assertString(final String actual) throws IOException
    {
        final String expected = Files.asCharSource(new File(SAMPLE_PATH), Charsets.UTF_8).read();
        assertEquals(expected, actual);
    }
}
