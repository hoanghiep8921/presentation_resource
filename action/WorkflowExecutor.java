package com.vnpay.workflow_engine.engine;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.core.entity.workflows.WorkflowNode;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.service.AbstractActionHandler;
import com.vnpay.workflow_engine.service.ActionHandlerFactory;
import com.vnpay.workflow_engine.service.WorkflowHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class WorkflowExecutor {

    private final ActionHandlerFactory actionHandlerFactory;
    private final WorkflowNavigator workflowNavigator;
    private final WorkflowHistoryService historyService;

    /**
     * BFS toàn workflow.
     *   - Lưu MEET_ENROLLMENT cho start node ngay khi vào.
     *   - Mỗi node action: chạy handler → save history → quyết định branch tiếp.
     *   - WAITING (DELAY) → dừng branch, sẽ resume bởi Kafka self-loop.
     *   - END_NODE → saveCompletion (kéo theo decrement counter, update ticket).
     *   - visited set chống cycle (ngay cả khi user vẽ sai hoặc GOTO_ACTION loop).
     */
    public void execute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        WorkflowNode startNode = context.getCurrentNode();

        if (startNode == null) {
            log.warn(
                    "Workflow has no start node. workflowId={}",
                    context.getWorkflow().getId()
            );
            return;
        }

        historyService.saveEnrollment(context, event);

        Queue<WorkflowNode> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();

        visited.add(startNode.getId());

        queue.addAll(
                workflowNavigator.nextNodes(
                        context.getWorkflow(),
                        startNode,
                        ActionResult.success()
                )
        );

        while (!queue.isEmpty()) {

            WorkflowNode node = queue.poll();

            if (node == null) {
                continue;
            }

            if (!visited.add(node.getId())) {

                log.warn(
                        "Detected duplicated node execution. nodeId={}",
                        node.getId()
                );

                continue;
            }

            processNode(context, node, event, queue);
        }
    }

    private void processNode(
            WorkflowContext rootContext,
            WorkflowNode node,
            WorkflowEvent event,
            Queue<WorkflowNode> queue
    ) {

        WorkflowContext context = new WorkflowContext(
                rootContext.getWorkflow(),
                node
        );

        if (workflowNavigator.isEndNode(node)) {
            historyService.saveCompletion(context, event);
            return;
        }

        AbstractActionHandler handler =
                actionHandlerFactory.getHandler(
                        node.getData().getActionType()
                );

        ActionResult result = handler.execute(context, event);

        historyService.save(context, event, result);

        if (result.isWaiting()) {

            log.info(
                    "Branch paused (WAITING). workflowId={}, nodeId={}",
                    rootContext.getWorkflow().getId(),
                    node.getId()
            );

            return;
        }

        List<WorkflowNode> nextNodes =
                workflowNavigator.nextNodes(
                        context.getWorkflow(),
                        node,
                        result
                );

        queue.addAll(nextNodes);
    }
}
