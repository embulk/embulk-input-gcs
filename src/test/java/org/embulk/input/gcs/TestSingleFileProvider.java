package org.embulk.input.gcs;

import org.junit.Test;

public class TestSingleFileProvider
{
    @Test
    public void testSeekableChannelReopener()
    {
        // TODO verify that it's able to resume from last error
        /*
         * 1. Use SingleFileProvider to receive ResumableInputStream
         * 2. Simulate IOException | RuntimeException on is#read()
         * 3. Assert that {@code #reopen()} is called
         * 4. Assert that reopener is called with proper offset
         * 5. Assert that InputStream was read fully
         */
    }
}
