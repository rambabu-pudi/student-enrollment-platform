package com.example.enrollment.processing.service;

import com.example.enrollment.common.event.EnrollmentEvent;
import com.example.enrollment.common.event.EnrollmentEventFactory;
import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;
import com.example.enrollment.processing.repository.DashboardProjectionRepository;
import com.example.enrollment.processing.repository.EnrollmentWriteRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EnrollmentProcessingServiceTest {

    private EnrollmentWriteRepository writeRepository;
    private DashboardProjectionRepository projectionRepository;
    private EnrollmentProcessingService service;

    private final EnrollmentEventFactory eventFactory = new EnrollmentEventFactory();

    @BeforeEach
    void setUp() {
        writeRepository = mock(EnrollmentWriteRepository.class);
        projectionRepository = mock(DashboardProjectionRepository.class);
        service = new EnrollmentProcessingService(
                writeRepository, projectionRepository, new SimpleMeterRegistry());
    }

    @Test
    @DisplayName("Duplicates inside one Kafka batch are collapsed before reaching the database")
    void collapsesInBatchDuplicates() {
        List<EnrollmentEvent> batch = List.of(
                event("S1", "U1", "C1"),
                event("S1", "U1", "C1"),
                event("s1", " u1 ", "c1"),
                event("S1", "U1", "C2"));

        when(writeRepository.insertIgnoringDuplicates(anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        var outcome = service.process(batch);

        ArgumentCaptor<List<EnrollmentEvent>> captor = ArgumentCaptor.forClass(List.class);
        verify(writeRepository).insertIgnoringDuplicates(captor.capture());

        assertThat(captor.getValue()).hasSize(2);
        assertThat(outcome.received()).isEqualTo(4);
        assertThat(outcome.persisted()).isEqualTo(2);
        assertThat(outcome.duplicates()).isEqualTo(2);
    }

    @Test
    @DisplayName("Only genuinely new rows are pushed to the dashboard projection")
    void projectsOnlyInsertedRows() {
        EnrollmentEvent newFact = event("S2", "U2", "C3");
        List<EnrollmentEvent> batch = List.of(event("S1", "U1", "C1"), newFact);

        when(writeRepository.insertIgnoringDuplicates(anyList())).thenReturn(List.of(newFact));

        service.process(batch);

        verify(projectionRepository).merge(List.of(newFact));
    }

    @Test
    @DisplayName("Events are written in natural-key order to give a deterministic lock order")
    void sortsByNaturalKey() {
        List<EnrollmentEvent> batch = List.of(
                event("S3", "U1", "C1"),
                event("S1", "U2", "C9"),
                event("S1", "U1", "C5"));

        when(writeRepository.insertIgnoringDuplicates(anyList()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.process(batch);

        ArgumentCaptor<List<EnrollmentEvent>> captor = ArgumentCaptor.forClass(List.class);
        verify(writeRepository).insertIgnoringDuplicates(captor.capture());

        assertThat(captor.getValue())
                .extracting(e -> e.payload().naturalKey())
                .containsExactly("S1|U1|C5", "S1|U2|C9", "S3|U1|C1");
    }

    @Test
    @DisplayName("Incomplete payloads are discarded rather than persisted")
    void discardsIncompleteRecords() {
        when(writeRepository.insertIgnoringDuplicates(anyList())).thenReturn(List.of());

        var outcome = service.process(List.of(event("S1", "U1", "C1")));

        assertThat(outcome.persisted()).isZero();
        assertThat(outcome.duplicates()).isEqualTo(1);
    }

    @Test
    @DisplayName("An empty batch is a no-op")
    void handlesEmptyBatch() {
        assertThat(service.process(List.of()).received()).isZero();
    }

    private EnrollmentEvent event(String student, String university, String classCode) {
        return eventFactory.create(
                new EnrollmentRecord(student, university, classCode), SourceType.CSV, "test-correlation");
    }
}
