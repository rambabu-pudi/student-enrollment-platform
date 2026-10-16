package com.example.enrollment.ingestion.parser;

import com.example.enrollment.common.model.EnrollmentRecord;

import java.util.Locale;
import java.util.Map;

/**
 * Tolerant field-name resolution shared by all parsers.
 *
 * <p>Real-world feeds disagree on casing and separators ({@code studentId}, {@code student_id},
 * {@code STUDENT-ID}). Normalising once here keeps every parser strategy thin and consistent.</p>
 */
final class FieldNames {

    static final String STUDENT = "studentid";
    static final String UNIVERSITY = "universityid";
    static final String CLASS = "classcode";

    /** Separators allowed inside a multi-valued class column. */
    private static final java.util.regex.Pattern MULTI_VALUE = java.util.regex.Pattern.compile("[;|,]");

    private FieldNames() {
    }

    /** Lower-cases and strips separators so {@code "Student_Id"} becomes {@code "studentid"}. */
    static String canonical(String rawName) {
        if (rawName == null) {
            return "";
        }
        return rawName.toLowerCase(Locale.ROOT)
                .replace("_", "")
                .replace("-", "")
                .replace(" ", "")
                .trim();
    }

    /** Maps alias spellings onto the three canonical field names. */
    static String resolve(String rawName) {
        String canonical = canonical(rawName);
        return switch (canonical) {
            case "studentid", "student", "sid", "studentcode" -> STUDENT;
            case "universityid", "university", "uid", "universitycode", "college" -> UNIVERSITY;
            case "classcode", "class", "cid", "classid", "course", "coursecode" -> CLASS;
            default -> canonical;
        };
    }

    /** Builds a record from a canonical field map, or {@code null} when a field is missing. */
    static EnrollmentRecord toRecord(Map<String, String> fields) {
        String student = fields.get(STUDENT);
        String university = fields.get(UNIVERSITY);
        String classCode = fields.get(CLASS);
        if (isBlank(student) || isBlank(university) || isBlank(classCode)) {
            return null;
        }
        return new EnrollmentRecord(student, university, classCode);
    }

    /**
     * Emits one record per class code. A single row may legitimately carry a multi-valued class
     * column such as {@code "C1|C2;C3"}; each value becomes its own canonical fact.
     *
     * @return number of records emitted (0 when the row is incomplete)
     */
    static int emit(Map<String, String> fields, java.util.function.Consumer<EnrollmentRecord> sink) {
        String student = fields.get(STUDENT);
        String university = fields.get(UNIVERSITY);
        String classes = fields.get(CLASS);
        if (isBlank(student) || isBlank(university) || isBlank(classes)) {
            return 0;
        }
        int emitted = 0;
        for (String classCode : MULTI_VALUE.split(classes)) {
            if (!isBlank(classCode)) {
                sink.accept(new EnrollmentRecord(student, university, classCode));
                emitted++;
            }
        }
        return emitted;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
