package com.example.enrollment.ingestion.parser;

import com.example.enrollment.common.exception.PayloadParseException;
import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * CSV strategy. Reads the header row to build a column index, then streams data rows.
 *
 * <p>Expected (header names are alias-tolerant and order-independent):</p>
 * <pre>
 * studentId,universityId,classCode
 * S1,U1,C1
 * S1,U1,C1      &lt;-- duplicate, collapsed downstream
 * S2,U2,"C1;C3" &lt;-- multi-valued class column, expands to two facts
 * </pre>
 */
@Component
public class CsvEnrollmentParser implements EnrollmentPayloadParser {

    private static final Logger log = LoggerFactory.getLogger(CsvEnrollmentParser.class);

    /** A malformed row must not abort a 10M-row file; bad rows are counted and skipped. */
    private static final int MAX_LOGGED_BAD_ROWS = 50;

    @Override
    public SourceType supportedSource() {
        return SourceType.CSV;
    }

    @Override
    public boolean supports(String contentType, String filename) {
        return (contentType != null && contentType.toLowerCase().contains("csv"))
                || (filename != null && filename.toLowerCase().endsWith(".csv"));
    }

    @Override
    public long parse(InputStream input, Consumer<EnrollmentRecord> sink) {
        long emitted = 0;
        int badRows = 0;
        try (CSVReader reader = new CSVReader(
                new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8), 1 << 16))) {

            String[] header = reader.readNext();
            if (header == null) {
                throw new PayloadParseException("CSV payload is empty: a header row is required");
            }
            Map<Integer, String> columns = indexHeader(header);
            if (!columns.containsValue(FieldNames.STUDENT)
                    || !columns.containsValue(FieldNames.UNIVERSITY)
                    || !columns.containsValue(FieldNames.CLASS)) {
                throw new PayloadParseException(
                        "CSV header must contain studentId, universityId and classCode columns");
            }

            String[] row;
            while ((row = reader.readNext()) != null) {
                Map<String, String> fields = new HashMap<>(4);
                for (Map.Entry<Integer, String> column : columns.entrySet()) {
                    int idx = column.getKey();
                    if (idx < row.length) {
                        fields.put(column.getValue(), row[idx]);
                    }
                }
                int produced = FieldNames.emit(fields, sink);
                if (produced == 0) {
                    badRows++;
                    if (badRows <= MAX_LOGGED_BAD_ROWS) {
                        log.warn("Skipping incomplete CSV row at line {}", reader.getLinesRead());
                    }
                }
                emitted += produced;
            }
        } catch (IOException | CsvValidationException e) {
            throw new PayloadParseException("Failed to read CSV payload: " + e.getMessage(), e);
        }
        if (badRows > 0) {
            log.warn("CSV ingestion skipped {} incomplete row(s)", badRows);
        }
        return emitted;
    }

    private Map<Integer, String> indexHeader(String[] header) {
        Map<Integer, String> columns = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            columns.put(i, FieldNames.resolve(header[i]));
        }
        return columns;
    }
}
