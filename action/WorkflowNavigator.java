package com.vnpay.workflow_engine.engine;

import com.vnpay.core.entity.workflows.WorkFlow;
import com.vnpay.core.entity.workflows.WorkflowEdge;
import com.vnpay.core.entity.workflows.WorkflowNode;
import com.vnpay.workflow_engine.constants.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
@Slf4j
public class WorkflowNavigator {

    public List<WorkflowNode> nextNodes(
            WorkFlow workflow,
            WorkflowNode currentNode,
            ActionResult result
    ) {

        boolean failed = result.isFailed();

        return workflow.getEdges()
                .stream()
                .filter(edge ->
                        edge.getSource().equals(currentNode.getId()))
                .filter(edge -> matchEdge(edge, failed))
                .map(edge -> findNode(workflow, edge.getTarget()))
                .filter(Objects::nonNull)
                .toList();
    }

    private boolean matchEdge(
            WorkflowEdge edge,
            boolean failed
    ) {

        if (failed) {
            return "red".equals(edge.getColor());
        }

        return !"red".equals(edge.getColor());
    }

    public WorkflowNode findNode(
            WorkFlow workflow,
            String nodeId
    ) {

        return workflow.getNodes()
                .stream()
                .filter(node -> node.getId().equals(nodeId))
                .findFirst()
                .orElse(null);
    }

    public boolean isEndNode(WorkflowNode node) {
        return Constant.END_NODE.equalsIgnoreCase(node.getType());
    }
}