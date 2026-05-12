package com.vnpay.workflow_engine.engine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vnpay.core.constant.Constants;
import com.vnpay.core.constant.EntityStatus;
import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.core.entity.workflows.Condition;
import com.vnpay.core.entity.workflows.WorkFlow;
import com.vnpay.core.entity.workflows.WorkFlowNoteData;
import com.vnpay.core.entity.workflows.WorkflowNode;
import com.vnpay.core.repository.workflows.WorkFlowsRepository;
import com.vnpay.core.utils.MDCUtil;
import com.vnpay.workflow_engine.constants.Constant;
import com.vnpay.workflow_engine.dto.TriggerWorkflowEvent;
import com.vnpay.workflow_engine.dto.WorkflowActionState;
import com.vnpay.workflow_engine.dto.WorkflowUpdateEvent;
import com.vnpay.workflow_engine.service.WorkflowCompletionService;
import com.vnpay.workflow_engine.service.WorkflowExecutionService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class WorkflowTriggerService {

    private static final Logger logger =
            LoggerFactory.getLogger(WorkflowTriggerService.class);

    private static final int MAX_RECURSION_DEPTH = 10;
    private static final Duration EXECUTION_LOCK_TTL = Duration.ofMinutes(10);

    private final WorkFlowsRepository workflowRepository;
    private final ConditionEvaluatorService conditionEvaluator;
    private final WorkflowExecutionService workflowExecutionService;
    private final WorkflowCompletionService completionService;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    private final MongoTemplate mongoTemplate;

    /**
     * Main workflow trigger entrypoint
     */
    public void handleTicketEvent(WorkflowEvent event) {

        String ticketId = getTicketId(event);
        String partnerId = getPartnerId(event);

        MDCUtil.setMDCContext(
                Optional.ofNullable(event.getTraceId()).orElse("WF"),
                ticketId
        );

        try {

            logger.info(
                    "Received workflow event | ticketId={} | action={} | objectType={}",
                    ticketId,
                    event.getAction(),
                    event.getObjectType()
            );

            validateEvent(event);

            List<String> executedWorkflowIds = getWorkflowIds(event);

            List<WorkFlow> candidateWorkflows =
                    workflowRepository
                            .findByPartnerIdAndTypeAndActivationTypeInAndStatusAndIdNotIn(
                                    partnerId,
                                    event.getObjectType(),
                                    Arrays.asList(Constant.ON_EVENT, Constant.ON_FILTER),
                                    EntityStatus.ACTIVE,
                                    executedWorkflowIds
                            );

            if (CollectionUtils.isEmpty(candidateWorkflows)) {
                logger.info(
                        "No workflow matched for ticketId={}",
                        ticketId
                );
                return;
            }

            logger.info(
                    "Found {} candidate workflows for ticketId={}",
                    candidateWorkflows.size(),
                    ticketId
            );

            // Distributed semaphore: tổng số workflow cần hoàn thành cho ticket
            // ở event này. Mỗi workflow xong/skip/error sẽ decrement key.
            completionService.initCounter(event, candidateWorkflows.size());

            for (WorkFlow workflow : candidateWorkflows) {

                try {

                    processWorkflow(workflow, event);

                } catch (Exception ex) {

                    logger.error(
                            "Workflow execution failed | workflowId={} | ticketId={}",
                            workflow.getId(),
                            ticketId,
                            ex
                    );

                    completionService.notifyWorkflowDone(event);
                }
            }

        } finally {
            MDCUtil.clearMDCContext();
        }
    }

    /**
     * Process single workflow safely
     */
    private void processWorkflow(
            WorkFlow workflow,
            WorkflowEvent event
    ) {

        String workflowId = workflow.getId();
        String ticketId = getTicketId(event);

        MDCUtil.setMDCContext(
                "WF_" + workflowId,
                ticketId
        );

        logger.info(
                "Processing workflow | workflowId={} | ticketId={}",
                workflowId,
                ticketId
        );

        WorkflowNode startNode = findStartNode(workflow);

        if (startNode == null || startNode.getData() == null) {

            logger.warn(
                    "Workflow has no valid start node | workflowId={}",
                    workflowId
            );

            completionService.notifyWorkflowDone(event);
            return;
        }

        WorkFlowNoteData nodeData = startNode.getData();

        boolean triggerMatched =
                isTriggerConditionMet(
                        nodeData.getActivationCondition(),
                        event,
                        false
                );

        if (!triggerMatched) {

            logger.debug(
                    "Activation condition not matched | workflowId={}",
                    workflowId
            );

            completionService.notifyWorkflowDone(event);
            return;
        }

        boolean filterMatched =
                isTriggerConditionMet(
                        nodeData.getFilterCondition(),
                        event,
                        true
                );

        if (!filterMatched) {

            logger.debug(
                    "Filter condition not matched | workflowId={}",
                    workflowId
            );

            completionService.notifyWorkflowDone(event);
            return;
        }

        String executionKey =
                buildExecutionKey(workflowId, ticketId, event);

        boolean acquired =
                acquireExecutionLock(executionKey);

        if (!acquired) {

            logger.warn(
                    "Duplicate workflow execution detected | workflowId={} | ticketId={}",
                    workflowId,
                    ticketId
            );

            return;
        }

        logger.info(
                "Workflow matched and execution started | workflowId={} | ticketId={}",
                workflowId,
                ticketId
        );

        workflowExecutionService.executeWorkflow(workflow, event);
    }

    /**
     * Prevent recursive workflow storm
     */
    private void validateEvent(WorkflowEvent event) {

        int depth = Optional.ofNullable(event.getDepth()).orElse(0);

        if (depth > MAX_RECURSION_DEPTH) {

            logger.error(
                    "Workflow recursion depth exceeded | depth={} | ticketId={}",
                    depth,
                    getTicketId(event)
            );

            throw new IllegalStateException(
                    "Workflow recursion exceeded max depth"
            );
        }
    }

    /**
     * Distributed idempotency lock
     */
    private boolean acquireExecutionLock(String executionKey) {

        try {

            Boolean success =
                    redisTemplate.opsForValue().setIfAbsent(
                            executionKey,
                            "1",
                            EXECUTION_LOCK_TTL
                    );

            return Boolean.TRUE.equals(success);

        } catch (Exception ex) {

            logger.error(
                    "Cannot acquire workflow lock | key={}",
                    executionKey,
                    ex
            );

            return false;
        }
    }

    /**
     * Build distributed execution key
     */
    private String buildExecutionKey(
            String workflowId,
            String ticketId,
            WorkflowEvent event
    ) {

        return String.format(
                "wf_exec:%s:%s:%s",
                workflowId,
                ticketId,
                Optional.ofNullable(event.getEventId())
                        .orElse(UUID.randomUUID().toString())
        );
    }

    /**
     * Trigger condition evaluator
     */
    public boolean isTriggerConditionMet(
            List<Condition> conditions,
            WorkflowEvent event,
            boolean isFilter
    ) {

        if (CollectionUtils.isEmpty(conditions)) {
            return true;
        }

        for (Condition condition : conditions) {

            try {

                boolean eventMatched =
                        event.isTestWorkflow()
                                || isEventTypeMatch(
                                event,
                                condition,
                                isFilter
                        );

                if (!eventMatched) {
                    continue;
                }

                boolean matched =
                        conditionEvaluator.evaluateConditionGroup(
                                condition,
                                event.getTicketData(),
                                event.getOldTicketData(),
                                isFilter
                        );

                if (matched) {
                    return true;
                }

            } catch (Exception ex) {

                logger.error(
                        "Condition evaluation failed",
                        ex
                );
            }
        }

        return false;
    }

    /**
     * Event type matcher
     */
    private static boolean isEventTypeMatch(
            WorkflowEvent event,
            Condition activationGroup,
            boolean isFilter
    ) {

        if (isFilter) {
            return true;
        }

        return
                (Constant.OBJECT_CREATED.equals(
                        activationGroup.getBaseOn())
                        && Constants.WorkflowAction.CREATED.equals(
                        event.getAction()))
                        ||
                        (Constant.ATTRIBUTE_CHANGED.equals(
                                activationGroup.getBaseOn())
                                && Constants.WorkflowAction.UPDATED.equals(
                                event.getAction()));
    }

    /**
     * Find workflow start node
     */
    public WorkflowNode findStartNode(WorkFlow workflow) {

        if (CollectionUtils.isEmpty(workflow.getNodes())) {
            return null;
        }

        return workflow.getNodes()
                .stream()
                .filter(node ->
                        workflow.getId().equals(node.getId()))
                .findFirst()
                .orElse(workflow.getNodes().get(0));
    }

    /**
     * Trigger workflow async event
     */
    @EventListener
    public void handleTriggerWorkflowEvent(
            TriggerWorkflowEvent event
    ) {

        try {

            workflowExecutionService.executeWorkflow(
                    event.getWorkflow(),
                    event.getWorkflowEvent()
            );

        } catch (Exception ex) {

            logger.error(
                    "TriggerWorkflowEvent failed",
                    ex
            );
        }
    }

    /**
     * Workflow chaining event
     */
    @EventListener
    public void handleTriggerEvent(
            WorkflowActionState event
    ) {

        if (event.getTicket() == null) {
            return;
        }

        if (!event.isUpdateTriggered()
                && !event.isCreateTriggered()) {
            return;
        }

        logger.info(
                "Trigger next workflow chain | ticketId={}",
                event.getTicket().getId()
        );

        Map<String, Object> ticketData =
                objectMapper.convertValue(
                        event.getTicket(),
                        new TypeReference<>() {
                        }
                );

        WorkflowEvent workflowEvent =
                WorkflowEvent.builder()
                        .eventId(UUID.randomUUID().toString())
                        .traceId(UUID.randomUUID().toString())
                        .objectType(Constants.TICKET)
                        .action(
                                event.isUpdateTriggered()
                                        ? Constants.WorkflowAction.UPDATED
                                        : Constants.WorkflowAction.CREATED
                        )
                        .ticketData(ticketData)
                        .depth(
                                Optional.ofNullable(event.getDepth())
                                        .orElse(0) + 1
                        )
                        .build();

        handleTicketEvent(workflowEvent);
    }

    /**
     * Atomic workflow objectIdNumber update
     */
    @EventListener
    public void handleWorkflowUpdateEvent(
            WorkflowUpdateEvent event
    ) {

        String workflowId = event.getWorkflowId();

        try {

            logger.info(
                    "Atomic increment objectIdNumber | workflowId={}",
                    workflowId
            );

            Query query =
                    new Query(
                            Criteria.where("_id").is(workflowId)
                    );

            Update update =
                    new Update().inc("objectIdNumber", 1L);

            mongoTemplate.updateFirst(
                    query,
                    update,
                    WorkFlow.class
            );

        } catch (Exception ex) {

            logger.error(
                    "Workflow objectId increment failed | workflowId={}",
                    workflowId,
                    ex
            );
        }
    }

    /**
     * Extract workflow ids safely
     */
    @SuppressWarnings("unchecked")
    private List<String> getWorkflowIds(
            WorkflowEvent event
    ) {

        try {

            return (List<String>)
                    event.getTicketData()
                            .getOrDefault(
                                    "workflowIds",
                                    new ArrayList<>()
                            );

        } catch (Exception ex) {

            logger.error(
                    "Cannot extract workflowIds",
                    ex
            );

            return new ArrayList<>();
        }
    }

    /**
     * Safe ticketId extractor
     */
    private String getTicketId(WorkflowEvent event) {

        return String.valueOf(
                event.getTicketData()
                        .getOrDefault("id", "")
        );
    }

    /**
     * Safe partnerId extractor
     */
    private String getPartnerId(WorkflowEvent event) {

        return String.valueOf(
                event.getTicketData()
                        .getOrDefault("partnerId", "")
        );
    }
}
