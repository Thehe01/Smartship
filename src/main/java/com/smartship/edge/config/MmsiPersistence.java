package com.smartship.edge.config;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@Slf4j
public final class MmsiPersistence {

    private static final String FILE_NAME = ".mmsi_cache";

    private MmsiPersistence() {
    }

    public static synchronized void write(String mmsi) {
        if (mmsi == null || mmsi.trim().isEmpty()) {
            return;
        }
        try {
            Path path = Path.of(FILE_NAME);
            Files.writeString(path, mmsi.trim(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.debug("[MMSI] Cached to local file {}: {}", FILE_NAME, mmsi);
        } catch (Exception e) {
            log.warn("[MMSI] Failed to write local cache: {}", e.getMessage());
        }
    }

    public static synchronized String read() {
        try {
            File f = new File(FILE_NAME);
            if (f.exists() && f.isFile()) {
                String content = Files.readString(f.toPath(), StandardCharsets.UTF_8).trim();
                if (!content.isEmpty()) {
                    return content;
                }
            }
        } catch (Exception e) {
            log.warn("[MMSI] Failed to read local cache: {}", e.getMessage());
        }
        return null;
    }
}
