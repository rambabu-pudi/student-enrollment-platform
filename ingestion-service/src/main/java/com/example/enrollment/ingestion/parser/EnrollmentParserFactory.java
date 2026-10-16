package com.example.enrollment.ingestion.parser;

import com.example.enrollment.common.exception.PayloadParseException;
import com.example.enrollment.common.model.SourceType;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Factory that resolves the right {@link EnrollmentPayloadParser} strategy.
 *
 * <p>Spring injects every strategy bean, so adding a new format (e.g. Avro, fixed-width)
 * means adding one class — no change here and no change to the controller. That is the
 * Open/Closed principle applied through Strategy + Factory.</p>
 */
@Component
public class EnrollmentParserFactory {

    private final List<EnrollmentPayloadParser> parsers;
    private final Map<SourceType, EnrollmentPayloadParser> bySource;

    public EnrollmentParserFactory(List<EnrollmentPayloadParser> parsers) {
        this.parsers = List.copyOf(parsers);
        this.bySource = parsers.stream()
                .collect(Collectors.toUnmodifiableMap(EnrollmentPayloadParser::supportedSource,
                        Function.identity()));
    }

    /** Content negotiation: pick a strategy from MIME type or file extension. */
    public EnrollmentPayloadParser resolve(String contentType, String filename) {
        return parsers.stream()
                .filter(parser -> parser.supports(contentType, filename))
                .findFirst()
                .orElseThrow(() -> new PayloadParseException(
                        "Unsupported payload format (contentType=%s, filename=%s). Supported: CSV, XML, JSON."
                                .formatted(contentType, filename)));
    }

    public EnrollmentPayloadParser forSource(SourceType source) {
        EnrollmentPayloadParser parser = bySource.get(source);
        if (parser == null) {
            throw new PayloadParseException("No parser registered for source " + source);
        }
        return parser;
    }
}
