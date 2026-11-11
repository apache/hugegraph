/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.raft;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.alipay.sofa.jraft.util.CRC64;

/**
 * Covers the archive step of PD raft snapshots: RaftStateMachine compares the checksum
 * computed while compressing with the one computed while decompressing.
 */
public class ZipUtilsTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testCompressDecompressRoundTripWithNestedDirectories() throws IOException {
        File root = tmp.newFolder("writer");
        File snapshot = new File(root, "snapshot");
        write(new File(snapshot, "CURRENT"), "MANIFEST-000001");
        write(new File(snapshot, "sst/000001.sst"), "data-1");
        write(new File(snapshot, "sst/deep/000002.sst"), "data-2");
        File archive = new File(root, "snapshot.zip");

        CRC64 compressSum = new CRC64();
        ZipUtils.compress(root.getPath(), "snapshot", archive.getPath(), compressSum);
        Assert.assertTrue(archive.length() > 0);
        byte[] archiveBytes = Files.readAllBytes(archive.toPath());
        CRC64 archiveSum = new CRC64();
        archiveSum.update(archiveBytes, 0, archiveBytes.length);
        Assert.assertEquals(archiveSum.getValue(), compressSum.getValue());

        File reader = tmp.newFolder("reader");
        CRC64 decompressSum = new CRC64();
        ZipUtils.decompress(archive.getPath(), reader.getPath(), decompressSum);

        Assert.assertEquals("MANIFEST-000001", read(new File(reader, "snapshot/CURRENT")));
        Assert.assertEquals("data-1", read(new File(reader, "snapshot/sst/000001.sst")));
        Assert.assertEquals("data-2", read(new File(reader, "snapshot/sst/deep/000002.sst")));
        // The follower verifies the archive with the checksum the leader recorded
        Assert.assertEquals(compressSum.getValue(), decompressSum.getValue());
    }

    @Test
    public void testChecksumDiffersForDifferentContent() throws IOException {
        Assert.assertNotEquals(checksumOf("data-1"), checksumOf("data-2"));
    }

    @Test
    public void testCompressEmptyDirectoryProducesEmptyArchive() throws IOException {
        File root = tmp.newFolder("empty-writer");
        Assert.assertTrue(new File(root, "snapshot").mkdir());
        File archive = new File(root, "snapshot.zip");

        ZipUtils.compress(root.getPath(), "snapshot", archive.getPath(), new CRC64());

        File reader = tmp.newFolder("empty-reader");
        ZipUtils.decompress(archive.getPath(), reader.getPath(), new CRC64());
        String[] extracted = reader.list();
        Assert.assertNotNull(extracted);
        Assert.assertEquals(0, extracted.length);
    }

    @Test
    public void testDecompressSkipsEntriesEscapingOutputDir() throws IOException {
        File base = tmp.newFolder("slip");
        File output = new File(base, "out");
        Assert.assertTrue(output.mkdir());
        File archive = new File(base, "evil.zip");
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archive))) {
            putEntry(zos, "../escaped.txt", "evil");
            // A sibling whose name shares the output dir prefix is outside it as well
            putEntry(zos, "../out-sibling/escaped.txt", "evil");
            putEntry(zos, "snapshot/kept.txt", "good");
        }

        ZipUtils.decompress(archive.getPath(), output.getPath(), new CRC64());

        Assert.assertFalse(new File(base, "escaped.txt").exists());
        Assert.assertFalse(new File(base, "out-sibling").exists());
        Assert.assertEquals("good", read(new File(output, "snapshot/kept.txt")));
    }

    @Test
    public void testDecompressMissingArchiveFails() {
        File missing = new File(tmp.getRoot(), "missing.zip");

        Assert.assertThrows(IOException.class,
                            () -> ZipUtils.decompress(missing.getPath(), tmp.getRoot().getPath(),
                                                      new CRC64()));
    }

    @Test
    public void testCompressMissingDirectoryFailsWithIOException() throws IOException {
        File root = tmp.newFolder("missing-writer");
        File archive = new File(root, "snapshot.zip");

        IOException exception = Assert.assertThrows(IOException.class,
                                                   () -> ZipUtils.compress(root.getPath(),
                                                                           "missing", archive.getPath(),
                                                                           new CRC64()));

        Assert.assertTrue(exception.getMessage().contains("missing"));
    }

    @Test
    public void testCompressRegularFileAsDirectoryFailsWithIOException() throws IOException {
        File root = tmp.newFolder("file-writer");
        write(new File(root, "snapshot"), "not a directory");
        File archive = new File(root, "snapshot.zip");

        Assert.assertThrows(IOException.class,
                            () -> ZipUtils.compress(root.getPath(), "snapshot", archive.getPath(),
                                                    new CRC64()));
    }

    private long checksumOf(String content) throws IOException {
        File root = tmp.newFolder();
        write(new File(root, "snapshot/file"), content);
        CRC64 checksum = new CRC64();
        ZipUtils.compress(root.getPath(), "snapshot", new File(root, "a.zip").getPath(),
                          checksum);
        return checksum.getValue();
    }

    private static void putEntry(ZipOutputStream zos, String name, String content)
            throws IOException {
        zos.putNextEntry(new ZipEntry(name));
        zos.write(content.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
    }

    private static void write(File file, String content) throws IOException {
        Files.createDirectories(file.getParentFile().toPath());
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(File file) throws IOException {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }
}
