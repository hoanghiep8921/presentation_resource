package com.vnpay.workflow_engine.service;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.core.entity.workflows.WorkFlowNoteData;
import com.vnpay.core.entity.workflows.WorkflowActionHistory;
import com.vnpay.core.entity.workflows.WorkflowNode;
import com.vnpay.core.repository.workflows.WorkflowActionHistoryRepository;
import com.vnpay.workflow_engine.constants.Constant;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.engine.ActionResult;
import com.vnpay.workflow_engine.engine.ActionStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class WorkflowHistoryService {

    private final WorkflowActionHistoryRepository repository;
    private final WorkflowCompletionService completionService;

    /**
     * Audit khi workflow vừa được kích hoạt: ghi nhận start node với
     * marker MEET_ENROLLMENT.
     */
    public void saveEnrollment(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        WorkflowActionHistory history =
                baseHistory(context, event)
                        .eventStatus(Constant.ModelStatus.ACTIVE)
                        .eventData(Map.of(Constant.MEET_ENROLLMENT, ""))
                        .build();

        persistOrCollect(history, event);
    }

    /**
     * Audit khi workflow đi tới END_NODE: lưu COMPLETE_WORKFLOW + bắn các
     * side effect hoàn thành (counter, ticket.workflowIds, objectIdNumber).
     */
    public void saveCompletion(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        WorkflowActionHistory history =
                baseHistory(context, event)
                        .eventStatus(Constant.ModelStatus.ACTIVE)
                        .eventData(Map.of(Constant.COMPLETE_WORKFLOW, ""))
                        .build();

        persistOrCollect(history, event);

        completionService.notifyWorkflowComplete(context, event);
    }

    /**
     * Audit cho 1 action node bất kỳ.
     * Nếu node lỗi → decrement counter (workflow coi như chấm dứt).
     */
    public void save(
            WorkflowContext context,
            WorkflowEvent event,
            ActionResult result
    ) {

        WorkflowActionHistory history =
                baseHistory(context, event)
                        .eventStatus(mapStatus(result.status()))
                        .eventData(result.data())
                        .build();

        persistOrCollect(history, event);

        if (result.isFailed()) {
            completionService.notifyWorkflowDone(event);
        }
    }

    /**
     * Test mode: KHÔNG ghi DB, gom vào list in-memory của event để
     * portal trả về UI dạng timeline preview (doc mục D.3).
     */
    private void persistOrCollect(
            WorkflowActionHistory history,
            WorkflowEvent event
    ) {

        if (event.isTestWorkflow()) {

            if (event.getActionHistory() == null) {
                log.warn("Test workflow but actionHistory list is null");
                return;
            }

            event.getActionHistory().add(history);
            return;
        }

        repository.save(history);
    }

    private WorkflowActionHistory.WorkflowActionHistoryBuilder baseHistory(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        WorkflowNode node = context.getCurrentNode();
        WorkFlowNoteData nodeData = node.getData();

        return WorkflowActionHistory.builder()
                .workflowId(context.getWorkflow().getId())
                .partnerId(context.getWorkflow().getPartnerId())
                .objectId(
                        (String) event.getTicketData()
                                .getOrDefault("id", "")
                )
                .objectType(context.getWorkflow().getType())
                .nodeId(node.getId())
                .actionType(
                        nodeData == null ? null : nodeData.getActionType()
                )
                .createdAt(LocalDateTime.now());
    }

    private Integer mapStatus(ActionStatus status) {

        return switch (status) {
            case SUCCESS, WAITING -> Constant.ModelStatus.ACTIVE;
            case FAILED -> Constant.ModelStatus.ERROR;
            case SKIPPED -> Constant.ModelStatus.INACTIVE;
        };
    }
}
