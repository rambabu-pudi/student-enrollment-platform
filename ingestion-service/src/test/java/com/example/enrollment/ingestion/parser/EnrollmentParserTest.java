package com.example.enrollment.ingestion.parser;

import com.example.enrollment.common.exception.PayloadParseException;
import com.example.enrollment.common.model.EnrollmentRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies that all three formats normalise to the identical canonical records. */
class EnrollmentParserTest {

    private final CsvEnrollmentParser csvParser = new CsvEnrollmentParser();
    private final XmlEnrollmentParser xmlParser = new XmlEnrollmentParser();
    private final JsonEnrollmentParser jsonParser = new JsonEnrollmentParser(new ObjectMapper());

    @Test
    @DisplayName("CSV: parses rows, tolerates alias headers and expands multi-valued class columns")
    void parsesCsv() {
        String csv = """
                student_id,University,classCode
                S1,U1,C1
                S1,U1,C2
                S2,U2,C1;C3
                """;

        List<EnrollmentRecord> records = collect(csvParser, csv);

        assertThat(records).containsExactly(
                new EnrollmentRecord("S1", "U1", "C1"),
                new EnrollmentRecord("S1", "U1", "C2"),
                new EnrollmentRecord("S2", "U2", "C1"),
                new EnrollmentRecord("S2", "U2", "C3"));
    }

    @Test
    @DisplayName("CSV: skips incomplete rows instead of failing the whole file")
    void skipsIncompleteCsvRows() {
        String csv = """
                studentId,universityId,classCode
                S1,U1,C1
                S2,,C1
                ,U2,C2
                S3,U3,
                """;

        List<EnrollmentRecord> records = collect(csvParser, csv);

        assertThat(records).containsExactly(new EnrollmentRecord("S1", "U1", "C1"));
    }

    @Test
    @DisplayName("CSV: a header missing a required column is rejected")
    void rejectsCsvWithoutRequiredColumns() {
        assertThatThrownBy(() -> collect(csvParser, "studentId,universityId\nS1,U1\n"))
                .isInstanceOf(PayloadParseException.class)
                .hasMessageContaining("classCode");
    }

    @Test
    @DisplayName("XML: parses element and attribute forms")
    void parsesXml() {
        String xml = """
                <enrollments>
                  <enrollment>
                    <studentId>S1</studentId>
                    <universityId>U1</universityId>
                    <classCode>C1</classCode>
                  </enrollment>
                  <enrollment studentId="S2" universityId="U2" classCode="C3"/>
                </enrollments>
                """;

        List<EnrollmentRecord> records = collect(xmlParser, xml);

        assertThat(records).containsExactly(
                new EnrollmentRecord("S1", "U1", "C1"),
                new EnrollmentRecord("S2", "U2", "C3"));
    }

    @Test
    @DisplayName("XML: external entities are disabled (XXE hardening)")
    void rejectsExternalEntities() {
        String xml = """
                <?xml version="1.0"?>
                <!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
                <enrollments>
                  <enrollment><studentId>&xxe;</studentId><universityId>U1</universityId><classCode>C1</classCode></enrollment>
                </enrollments>
                """;

        assertThatThrownBy(() -> collect(xmlParser, xml))
                .isInstanceOf(PayloadParseException.class);
    }

    @Test
    @DisplayName("JSON: accepts a bare array")
    void parsesJsonArray() {
        String json = """
                [
                  {"studentId":"S1","universityId":"U1","classCode":"C1"},
                  {"studentId":"S1","universityId":"U2","classCode":"C3"}
                ]
                """;

        List<EnrollmentRecord> records = collect(jsonParser, json);

        assertThat(records).containsExactly(
                new EnrollmentRecord("S1", "U1", "C1"),
                new EnrollmentRecord("S1", "U2", "C3"));
    }

    @Test
    @DisplayName("JSON: accepts an envelope with a classes array, expanding one object into many facts")
    void parsesJsonEnvelopeWithClassArray() {
        String json = """
                {"correlationId":"abc",
                 "records":[{"studentId":"S1","universityId":"U1","classes":["C1","C2","C3"]}]}
                """;

        List<EnrollmentRecord> records = collect(jsonParser, json);

        assertThat(records).containsExactly(
                new EnrollmentRecord("S1", "U1", "C1"),
                new EnrollmentRecord("S1", "U1", "C2"),
                new EnrollmentRecord("S1", "U1", "C3"));
    }

    @Test
    @DisplayName("All formats normalise casing and whitespace to the same canonical key")
    void normalisesAcrossFormats() {
        List<EnrollmentRecord> fromCsv = collect(csvParser, "studentId,universityId,classCode\n s1 ,u1,c1\n");
        List<EnrollmentRecord> fromXml = collect(xmlParser,
                "<enrollments><enrollment><studentId>S1</studentId>"
                        + "<universityId>U1</universityId><classCode>C1</classCode></enrollment></enrollments>");
        List<EnrollmentRecord> fromJson = collect(jsonParser,
                "[{\"studentId\":\"S1\",\"universityId\":\"u1\",\"classCode\":\" C1 \"}]");

        assertThat(fromCsv).isEqualTo(fromXml).isEqualTo(fromJson);
        assertThat(fromCsv.getFirst().naturalKey()).isEqualTo("S1|U1|C1");
    }

    private List<EnrollmentRecord> collect(EnrollmentPayloadParser parser, String payload) {
        List<EnrollmentRecord> records = new ArrayList<>();
        InputStream input = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));
        long emitted = parser.parse(input, records::add);
        assertThat(emitted).isEqualTo(records.size());
        return records;
    }
}
