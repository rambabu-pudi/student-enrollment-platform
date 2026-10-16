package com.example.enrollment.ingestion.parser;

import com.example.enrollment.common.exception.PayloadParseException;
import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * JSON strategy using Jackson's streaming API.
 *
 * <p>Accepts either a bare array or an envelope with a {@code records}/{@code enrollments} array:</p>
 * <pre>{@code
 * [ { "studentId": "S1", "universityId": "U1", "classCode": "C1" } ]
 * { "records": [ { "studentId": "S1", "universityId": "U1", "classes": ["C1","C2"] } ] }
 * }</pre>
 *
 * <p>The {@code classes} array form is supported so a caller can express
 * "S1 at U1 takes C1 and C2" in a single object.</p>
 */
@Component
public class JsonEnrollmentParser implements EnrollmentPayloadParser {

    private static final Logger log = LoggerFactory.getLogger(JsonEnrollmentParser.class);

    private final JsonFactory jsonFactory;

    public JsonEnrollmentParser(ObjectMapper objectMapper) {
        this.jsonFactory = objectMapper.getFactory();
    }

    @Override
    public SourceType supportedSource() {
        return SourceType.JSON_API;
    }

    @Override
    public boolean supports(String contentType, String filename) {
        return (contentType != null && contentType.toLowerCase().contains("json"))
                || (filename != null && filename.toLowerCase().endsWith(".json"));
    }

    @Override
    public long parse(InputStream input, Consumer<EnrollmentRecord> sink) {
        long emitted = 0;
        int badRecords = 0;
        try (JsonParser parser = jsonFactory.createParser(input)) {
            JsonToken token = parser.nextToken();
            if (token == JsonToken.START_OBJECT) {
                token = advanceToRecordsArray(parser);
            }
            if (token != JsonToken.START_ARRAY) {
                throw new PayloadParseException(
                        "JSON payload must be an array of records or an object containing a 'records' array");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                int produced = readObject(parser, sink);
                if (produced == 0) {
                    badRecords++;
                }
                emitted += produced;
            }
        } catch (IOException e) {
            throw new PayloadParseException("Failed to read JSON payload: " + e.getMessage(), e);
        }
        if (badRecords > 0) {
            log.warn("JSON ingestion skipped {} incomplete record(s)", badRecords);
        }
        return emitted;
    }

    private JsonToken advanceToRecordsArray(JsonParser parser) throws IOException {
        while (parser.nextToken() != null) {
            if (parser.currentToken() == JsonToken.FIELD_NAME) {
                String field = FieldNames.canonical(parser.currentName());
                if ("records".equals(field) || "enrollments".equals(field) || "data".equals(field)) {
                    return parser.nextToken();
                }
                parser.nextToken();
                parser.skipChildren();
            }
        }
        return null;
    }

    private int readObject(JsonParser parser, Consumer<EnrollmentRecord> sink) throws IOException {
        Map<String, String> fields = new HashMap<>(4);
        StringBuilder classes = new StringBuilder();

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String field = FieldNames.resolve(parser.currentName());
            JsonToken value = parser.nextToken();
            if (value == JsonToken.START_ARRAY) {
                boolean isClassArray = FieldNames.CLASS.equals(field) || "classes".equals(field);
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() == JsonToken.START_OBJECT
                            || parser.currentToken() == JsonToken.START_ARRAY) {
                        parser.skipChildren();
                        continue;
                    }
                    if (isClassArray) {
                        if (!classes.isEmpty()) {
                            classes.append(';');
                        }
                        classes.append(parser.getValueAsString(""));
                    }
                }
            } else if (value == JsonToken.START_OBJECT) {
                parser.skipChildren();
            } else {
                fields.put(field, parser.getValueAsString());
            }
        }
        if (!classes.isEmpty()) {
            fields.put(FieldNames.CLASS, classes.toString());
        }
        return FieldNames.emit(fields, sink);
    }
}
