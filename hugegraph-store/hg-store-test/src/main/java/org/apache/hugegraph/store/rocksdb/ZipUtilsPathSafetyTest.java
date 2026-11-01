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

package org.apache.hugegraph.store.rocksdb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hugegraph.rocksdb.access.util.CRC64;
import org.apache.hugegraph.rocksdb.access.util.ZipUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Snapshot archives are produced and unpacked by two ZipUtils copies: the RocksDB one used for
 * partition snapshots and the core one. Both must keep extracted files inside the target dir.
 */
public class ZipUtilsPathSafetyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static void write(File file, String content) throws IOException {
        Files.createDirectories(file.getParentFile().toPath());
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(File file) throws IOException {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    private File maliciousZip(String... entryNames) throws IOException {
        File zip = this.folder.newFile();
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zip))) {
            zos.putNextEntry(new ZipEntry("snapshot/ok.sst"));
            zos.write("ok".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            for (String name : entryNames) {
                zos.putNextEntry(new ZipEntry(name));
                zos.write("pwned".getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        return zip;
    }

    @Test
    public void testRoundTripKeepsNestedLayoutAndChecksumsAgree() throws IOException {
        File root = this.folder.newFolder("root");
        write(new File(root, "snapshot/CURRENT"), "MANIFEST-000001");
        write(new File(root, "snapshot/sub/dir/000004.sst"), "sst-bytes");
        Files.createDirectories(new File(root, "snapshot/empty").toPath());
        File zip = new File(this.folder.getRoot(), "snapshot.zip");
        File out = this.folder.newFolder("out");

        CRC64 written = new CRC64();
        ZipUtils.compress(root.getAbsolutePath(), "snapshot", zip.getAbsolutePath(), written);
        CRC64 read = new CRC64();
        ZipUtils.decompress(zip.getAbsolutePath(), out.getAbsolutePath(), read);

        assertEquals("MANIFEST-000001", read(new File(out, "snapshot/CURRENT")));
        assertEquals("sst-bytes", read(new File(out, "snapshot/sub/dir/000004.sst")));
        // The checksum covers the whole archive on both sides, so it detects transfer damage
        assertTrue(written.getValue() != 0L);
        assertEquals(written.getValue(), read.getValue());

        File coreOut = this.folder.newFolder("core-out");
        CRC32 coreRead = new CRC32();
        org.apache.hugegraph.store.util.ZipUtils.decompress(zip.getAbsolutePath(),
                                                            coreOut.getAbsolutePath(), coreRead);
        assertArrayEquals(Files.readAllBytes(new File(out, "snapshot/sub/dir/000004.sst").toPath()),
                          Files.readAllBytes(new File(coreOut, "snapshot/sub/dir/000004.sst").toPath()));
    }

    @Test
    public void testRocksDbDecompressSkipsEntriesEscapingTheTargetDir() throws IOException {
        File out = this.folder.newFolder("out");
        String absoluteEntry = new File(this.folder.getRoot(), "absolute.txt")
                .getAbsolutePath().replace(File.separatorChar, '/');
        assertFalse(out.toPath().resolve(absoluteEntry).normalize().startsWith(out.toPath()));
        File zip = maliciousZip("../escaped.txt", "snapshot/../../escaped2.txt", "../out-evil/x.txt",
                                absoluteEntry);

        ZipUtils.decompress(zip.getAbsolutePath(), out.getAbsolutePath(), new CRC32());

        assertEquals("ok", read(new File(out, "snapshot/ok.sst")));
        assertFalse(new File(this.folder.getRoot(), "escaped.txt").exists());
        assertFalse(new File(this.folder.getRoot(), "escaped2.txt").exists());
        assertFalse(new File(this.folder.getRoot(), "out-evil").exists());
        assertFalse(new File(this.folder.getRoot(), "absolute.txt").exists());
    }

    @Test
    public void testCoreDecompressRejectsEntriesEscapingTheTargetDir() throws IOException {
        File out = this.folder.newFolder("out");
        // "../out-evil" shares the textual prefix of the target dir and must still be rejected
        for (String evil : new String[]{"../escaped.txt", "snapshot/../../escaped.txt",
                                        "../out-evil/x.txt"}) {
            File zip = maliciousZip(evil);
            IOException e = assertThrows(IOException.class, () ->
                    org.apache.hugegraph.store.util.ZipUtils.decompress(
                            zip.getAbsolutePath(), out.getAbsolutePath(), new CRC32()));
            assertTrue(e.getMessage(), e.getMessage().contains("outside of the target dir"));
            assertFalse(new File(this.folder.getRoot(), "escaped.txt").exists());
            assertFalse(new File(this.folder.getRoot(), "out-evil").exists());
            // Each attack has its own archive; TemporaryFolder owns cleanup.
        }
    }

    @Test
    public void testCoreZipUtilsValidatesArguments() throws IOException {
        String root = this.folder.newFolder("root").getAbsolutePath();
        String zip = new File(this.folder.getRoot(), "x.zip").getAbsolutePath();
        String out = this.folder.newFolder("out").getAbsolutePath();

        assertThrows(IllegalArgumentException.class, () ->
                org.apache.hugegraph.store.util.ZipUtils.compress(null, "s", zip, new CRC32()));
        assertThrows(IllegalArgumentException.class, () ->
                org.apache.hugegraph.store.util.ZipUtils.compress(root, "s", zip, null));
        assertThrows(IllegalArgumentException.class, () ->
                org.apache.hugegraph.store.util.ZipUtils.decompress(zip, null, new CRC32()));

        IOException missingDir = assertThrows(IOException.class, () ->
                org.apache.hugegraph.store.util.ZipUtils.compress(root, "missing", zip, new CRC32()));
        assertTrue(missingDir.getMessage().contains("Source directory does not exist"));
        assertFalse("no archive may be created for a missing source", new File(zip).exists());

        IOException missingZip = assertThrows(IOException.class, () ->
                org.apache.hugegraph.store.util.ZipUtils.decompress(zip, out, new CRC32()));
        assertTrue(missingZip.getMessage().contains("Source file does not exist"));
    }
}
