package com.example.enrollment.ingestion.parser;

import com.example.enrollment.common.exception.PayloadParseException;
import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * XML strategy backed by StAX pull parsing — constant memory regardless of document size,
 * unlike DOM/JAXB which would materialise the whole tree.
 *
 * <p>Expected shape (element names are alias-tolerant):</p>
 * <pre>{@code
 * <enrollments>
 *   <enrollment>
 *     <studentId>S1</studentId>
 *     <universityId>U1</universityId>
 *     <classCode>C1</classCode>
 *   </enrollment>
 * </enrollments>
 * }</pre>
 *
 * <p>Security: DTDs and external entities are disabled, closing the XXE and billion-laughs
 * attack surface. This is mandatory for any externally reachable XML endpoint.</p>
 */
@Component
public class XmlEnrollmentParser implements EnrollmentPayloadParser {

    private static final Logger log = LoggerFactory.getLogger(XmlEnrollmentParser.class);
    private static final String RECORD_ELEMENT = "enrollment";

    private final XMLInputFactory factory = hardenedFactory();

    private static XMLInputFactory hardenedFactory() {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
        return factory;
    }

    @Override
    public SourceType supportedSource() {
        return SourceType.XML;
    }

    @Override
    public boolean supports(String contentType, String filename) {
        return (contentType != null && contentType.toLowerCase().contains("xml"))
                || (filename != null && filename.toLowerCase().endsWith(".xml"));
    }

    @Override
    public long parse(InputStream input, Consumer<EnrollmentRecord> sink) {
        long emitted = 0;
        int badRecords = 0;
        XMLStreamReader reader = null;
        try {
            reader = factory.createXMLStreamReader(input, "UTF-8");
            Map<String, String> fields = null;
            String currentField = null;
            StringBuilder text = new StringBuilder();

            while (reader.hasNext()) {
                int event = reader.next();
                switch (event) {
                    case XMLStreamConstants.START_ELEMENT -> {
                        String local = reader.getLocalName();
                        if (RECORD_ELEMENT.equalsIgnoreCase(local)) {
                            fields = new HashMap<>(4);
                            readAttributes(reader, fields);
                        } else if (fields != null) {
                            currentField = FieldNames.resolve(local);
                            text.setLength(0);
                        }
                    }
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
                        if (currentField != null) {
                            text.append(reader.getText());
                        }
                    }
                    case XMLStreamConstants.END_ELEMENT -> {
                        String local = reader.getLocalName();
                        if (RECORD_ELEMENT.equalsIgnoreCase(local)) {
                            if (fields != null) {
                                int produced = FieldNames.emit(fields, sink);
                                if (produced == 0) {
                                    badRecords++;
                                }
                                emitted += produced;
                            }
                            fields = null;
                            currentField = null;
                        } else if (fields != null && currentField != null) {
                            fields.put(currentField, text.toString().trim());
                            currentField = null;
                            text.setLength(0);
                        }
                    }
                    default -> {
                        // comments, processing instructions and whitespace are irrelevant here
                    }
                }
            }
        } catch (XMLStreamException e) {
            throw new PayloadParseException("Failed to read XML payload: " + e.getMessage(), e);
        } finally {
            closeQuietly(reader);
        }
        if (badRecords > 0) {
            log.warn("XML ingestion skipped {} incomplete <enrollment> element(s)", badRecords);
        }
        return emitted;
    }

    /** Supports the attribute form {@code <enrollment studentId="S1" .../>} as well. */
    private void readAttributes(XMLStreamReader reader, Map<String, String> fields) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            fields.put(FieldNames.resolve(reader.getAttributeLocalName(i)), reader.getAttributeValue(i));
        }
    }

    private void closeQuietly(XMLStreamReader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (XMLStreamException e) {
                log.debug("Ignoring XMLStreamReader close failure", e);
            }
        }
    }
}
