package com.vnpay.workflow_engine.service;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.core.entity.workflows.WorkflowActionHistory;
import com.vnpay.core.entity.workflows.WorkflowNode;
import com.vnpay.core.repository.workflows.WorkflowActionHistoryRepository;
import com.vnpay.workflow_engine.constants.Constant;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.engine.ActionResult;
import com.vnpay.workflow_engine.engine.ActionStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class WorkflowHistoryService {

    private final WorkflowActionHistoryRepository repository;
    private final ApplicationEventPublisher publisher;

    public void save(
            WorkflowContext context,
            WorkflowEvent event,
            ActionResult result
    ) {

        WorkflowNode node = context.getCurrentNode();

        WorkflowActionHistory history = WorkflowActionHistory.builder()
                .workflowId(context.getWorkflow().getId())
                .partnerId(context.getWorkflow().getPartnerId())
                .objectId((String) event.getTicketData().getOrDefault("id", ""))
                .objectType(context.getWorkflow().getType())
                .nodeId(node.getId())
                .actionType(node.getData().getActionType())
                .eventData(result.data())
                .eventStatus(mapStatus(result.status()))
                .createdAt(LocalDateTime.now())
                .build();

        repository.save(history);
    }

    public void completeWorkflow(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        log.info(
                "Workflow completed. workflowId={}, ticketId={}",
                context.getWorkflow().getId(),
                event.getTicketData().get("id")
        );
    }

    private Integer mapStatus(ActionStatus status) {

        return switch (status) {
            case SUCCESS -> Constant.ModelStatus.ACTIVE;
            case FAILED -> Constant.ModelStatus.ERROR;
            case SKIPPED -> Constant.ModelStatus.INACTIVE;
        };
    }
}