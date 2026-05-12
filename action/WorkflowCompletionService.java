package com.vnpay.workflow_engine.service;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.dto.WorkflowUpdateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

/**
 * Quản lý vòng đời "đã xong" của 1 workflow trên 1 ticket:
 *   1. Set semaphore `wf_counter` khi trigger biết có N workflow ứng viên.
 *   2. Decrement counter mỗi khi 1 workflow đi tới END_NODE / ERROR / skip
 *      → khi về 0 thì coi như "mọi workflow của ticket đã xử lý xong".
 *   3. Khi 1 workflow COMPLETE: append vào ticket.workflowIds[] (idempotency)
 *      + publish WorkflowUpdateEvent ($inc objectIdNumber).
 *
 * Tách thành service riêng để Trigger / Executor / HistoryService không
 * phải biết chi tiết Redis / Mongo / event bus.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class WorkflowCompletionService {

    private static final Duration COUNTER_TTL = Duration.ofDays(10);
    private static final String TICKETS_COLLECTION = "tickets";

    private final StringRedisTemplate redisTemplate;
    private final MongoTemplate mongoTemplate;
    private final ApplicationEventPublisher publisher;

    /**
     * Khởi tạo semaphore tại thời điểm trigger.
     */
    public void initCounter(WorkflowEvent event, int total) {

        if (event.isTestWorkflow() || total <= 0) {
            return;
        }

        String key = counterKey(event);

        try {

            redisTemplate.opsForValue().set(
                    key,
                    String.valueOf(total),
                    COUNTER_TTL
            );

            log.debug(
                    "Workflow counter initialized | key={} | total={}",
                    key,
                    total
            );

        } catch (Exception ex) {

            log.error(
                    "Cannot init workflow counter | key={}",
                    key,
                    ex
            );
        }
    }

    /**
     * Workflow đã chạm END_NODE: ghi nhận đầy đủ side effects.
     */
    public void notifyWorkflowComplete(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        if (event.isTestWorkflow()) {
            return;
        }

        String ticketId = ticketId(event);
        String workflowId = context.getWorkflow().getId();

        appendWorkflowIdToTicket(ticketId, workflowId);

        publisher.publishEvent(
                WorkflowUpdateEvent.builder()
                        .workflowId(workflowId)
                        .build()
        );

        decrementCounter(event);
    }

    /**
     * Workflow kết thúc nhưng không phải qua END_NODE
     * (skip vì condition fail, error trong khi chạy, không có start node, ...).
     */
    public void notifyWorkflowDone(WorkflowEvent event) {

        if (event.isTestWorkflow()) {
            return;
        }

        decrementCounter(event);
    }

    private void decrementCounter(WorkflowEvent event) {

        String key = counterKey(event);

        try {

            Long remaining =
                    redisTemplate.opsForValue().decrement(key);

            log.debug(
                    "Workflow counter decremented | key={} | remaining={}",
                    key,
                    remaining
            );

            if (remaining != null && remaining <= 0L) {

                redisTemplate.delete(key);

                log.info(
                        "All workflows finished for ticket | key={}",
                        key
                );
            }

        } catch (Exception ex) {

            log.error(
                    "Cannot decrement workflow counter | key={}",
                    key,
                    ex
            );
        }
    }

    private void appendWorkflowIdToTicket(
            String ticketId,
            String workflowId
    ) {

        if (ticketId == null || ticketId.isEmpty()) {
            return;
        }

        try {

            Query query =
                    new Query(Criteria.where("_id").is(ticketId));

            Update update =
                    new Update().addToSet("workflowIds", workflowId);

            mongoTemplate.updateFirst(
                    query,
                    update,
                    TICKETS_COLLECTION
            );

        } catch (Exception ex) {

            log.error(
                    "Cannot update ticket.workflowIds | ticketId={} | workflowId={}",
                    ticketId,
                    workflowId,
                    ex
            );
        }
    }

    private static String counterKey(WorkflowEvent event) {

        return String.format(
                "wf_counter:%s:%s",
                Optional.ofNullable(event.getAction()).orElse(""),
                ticketId(event)
        );
    }

    private static String ticketId(WorkflowEvent event) {

        return Optional.ofNullable(event.getTicketData())
                .map(data -> data.get("id"))
                .map(Object::toString)
                .orElse("");
    }
}
