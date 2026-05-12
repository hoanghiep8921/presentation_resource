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

import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class WorkflowExecutor {

    private final ActionHandlerFactory actionHandlerFactory;
    private final WorkflowNavigator workflowNavigator;
    private final WorkflowHistoryService historyService;

    public void execute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        Queue<WorkflowNode> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();

        queue.add(context.getCurrentNode());

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

            historyService.completeWorkflow(context, event);

            return;
        }

        String actionType = node.getData().getActionType();

        AbstractActionHandler handler =
                actionHandlerFactory.getHandler(actionType);

        ActionResult result =
                handler.execute(context, event);

        historyService.save(context, event, result);

        List<WorkflowNode> nextNodes =
                workflowNavigator.nextNodes(
                        context.getWorkflow(),
                        node,
                        result
                );

        queue.addAll(nextNodes);
    }
}