package com.example.enrollment.processing.service;

import com.example.enrollment.common.event.EnrollmentEvent;
import com.example.enrollment.processing.repository.DashboardProjectionRepository;
import com.example.enrollment.processing.repository.EnrollmentWriteRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Applies a batch of enrollment events to the write model and the read model in one transaction.
 *
 * <p>Deduplication happens twice, deliberately:</p>
 * <ol>
 *   <li><b>In-batch</b> — a {@code HashSet} of deterministic event ids collapses repeats inside
 *       the Kafka poll, so the batch sent to Postgres carries no self-conflicts.</li>
 *   <li><b>In-database</b> — {@code ON CONFLICT DO NOTHING} on the natural-key primary key is
 *       the authoritative, cross-pod, cross-file, cross-source guarantee.</li>
 * </ol>
 *
 * <p>Events are sorted by natural key before being written. Combined with partitioning the
 * Kafka topic by {@code studentId} (so one student is only ever handled by one consumer), this
 * gives a global lock-acquisition order and removes the deadlock risk that concurrent,
 * arbitrarily-ordered upsert batches would otherwise create.</p>
 */
@Service
public class EnrollmentProcessingService {

    private static final Logger log = LoggerFactory.getLogger(EnrollmentProcessingService.class);

    private static final Comparator<EnrollmentEvent> NATURAL_KEY_ORDER =
            Comparator.comparing((EnrollmentEvent e) -> e.payload().studentId())
                    .thenComparing(e -> e.payload().universityId())
                    .thenComparing(e -> e.payload().classCode());

    private final EnrollmentWriteRepository writeRepository;
    private final DashboardProjectionRepository projectionRepository;
    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Timer batchTimer;

    public EnrollmentProcessingService(EnrollmentWriteRepository writeRepository,
                                       DashboardProjectionRepository projectionRepository,
                                       MeterRegistry meterRegistry) {
        this.writeRepository = writeRepository;
        this.projectionRepository = projectionRepository;
        this.processedCounter = Counter.builder("enrollment.processing.persisted")
                .description("Newly persisted enrollment facts")
                .register(meterRegistry);
        this.duplicateCounter = Counter.builder("enrollment.processing.duplicates")
                .description("Events discarded as duplicates")
                .register(meterRegistry);
        this.batchTimer = Timer.builder("enrollment.processing.batch.duration")
                .description("Duration of one persisted batch")
                .register(meterRegistry);
    }

    /**
     * Write model and read model are updated in a single transaction so the dashboard can
     * never show a class that is not in the authoritative table, or vice versa.
     */
    @Transactional
    public BatchOutcome process(List<EnrollmentEvent> events) {
        if (events == null || events.isEmpty()) {
            return BatchOutcome.empty();
        }
        return batchTimer.record(() -> {
            List<EnrollmentEvent> distinct = collapseInBatchDuplicates(events);
            distinct.sort(NATURAL_KEY_ORDER);

            List<EnrollmentEvent> inserted = writeRepository.insertIgnoringDuplicates(distinct);
            projectionRepository.merge(inserted);

            long duplicates = events.size() - inserted.size();
            processedCounter.increment(inserted.size());
            duplicateCounter.increment(duplicates);

            if (log.isDebugEnabled()) {
                log.debug("Batch processed received={} distinct={} persisted={} duplicates={}",
                        events.size(), distinct.size(), inserted.size(), duplicates);
            }
            return new BatchOutcome(events.size(), inserted.size(), duplicates);
        });
    }

    private List<EnrollmentEvent> collapseInBatchDuplicates(List<EnrollmentEvent> events) {
        Set<UUID> seen = new HashSet<>(Math.max(16, events.size() * 2));
        List<EnrollmentEvent> distinct = new ArrayList<>(events.size());
        for (EnrollmentEvent event : events) {
            if (event != null && event.payload() != null && event.payload().isComplete()
                    && seen.add(event.eventId())) {
                distinct.add(event);
            }
        }
        return distinct;
    }

    /** Result of processing one batch. */
    public record BatchOutcome(int received, int persisted, long duplicates) {
        static BatchOutcome empty() {
            return new BatchOutcome(0, 0, 0);
        }
    }
}
