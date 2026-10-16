package com.example.enrollment.ingestion.parser;

import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;

import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Strategy interface for turning a raw inbound payload into canonical records.
 *
 * <p>Implementations MUST stream: they are handed an {@link InputStream} and push each parsed
 * record into the supplied sink rather than materialising a list. This keeps memory flat for
 * multi-hundred-megabyte files and is what allows a single pod to sustain the 1M records/hour
 * target without heap pressure.</p>
 */
public interface EnrollmentPayloadParser {

    /** The source channel this strategy handles. */
    SourceType supportedSource();

    /**
     * @param contentType MIME type from the request, may be {@code null}
     * @param filename    original filename, may be {@code null}
     * @return true when this strategy can handle the payload
     */
    boolean supports(String contentType, String filename);

    /**
     * Streams the payload, pushing every syntactically valid record into {@code sink}.
     *
     * @return number of records emitted to the sink
     */
    long parse(InputStream input, Consumer<EnrollmentRecord> sink);
}
