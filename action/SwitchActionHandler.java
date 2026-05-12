package com.vnpay.workflow_engine.service.actions;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.core.entity.workflows.SwitchCondition;
import com.vnpay.core.entity.workflows.WorkFlowNoteData;
import com.vnpay.core.entity.workflows.WorkflowNode;
import com.vnpay.workflow_engine.constants.Constant;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.engine.ActionResult;
import com.vnpay.workflow_engine.engine.ConditionEvaluatorService;
import com.vnpay.workflow_engine.engine.WorkflowNavigator;
import com.vnpay.workflow_engine.service.AbstractActionHandler;
import com.vnpay.workflow_engine.utils.CriteriaUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class SwitchActionHandler extends AbstractActionHandler {

    private final ConditionEvaluatorService conditionEvaluator;
    private final WorkflowNavigator workflowNavigator;

    @Override
    public String getActionType() {
        return Constant.SWITCH;
    }

    @Override
    protected ActionResult doExecute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        WorkflowNode currentNode = context.getCurrentNode();
        WorkFlowNoteData nodeData = currentNode.getData();

        String subActionType = nodeData.getSubActionType();

        log.info(
                "Execute switch node. workflowId={}, nodeId={}, subType={}",
                context.getWorkflow().getId(),
                currentNode.getId(),
                subActionType
        );

        if (Constant.SWITCH_ON_FILTER.equals(subActionType)) {
            return handleFilterSwitch(context, event);
        }

        if (Constant.SWITCH_ON_PERCENT.equals(subActionType)) {
            return handlePercentSwitch(context, event);
        }

        log.warn(
                "Unsupported switch subType={}",
                subActionType
        );

        return ActionResult.skipped();
    }

    private ActionResult handleFilterSwitch(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        WorkFlowNoteData nodeData =
                context.getCurrentNode().getData();

        List<SwitchCondition> conditions =
                nodeData.getSwitchCondition();

        if (CollectionUtils.isEmpty(conditions)) {

            log.warn(
                    "Switch condition empty. nodeId={}",
                    context.getCurrentNode().getId()
            );

            return ActionResult.skipped();
        }

        CriteriaUtil.sortSwitchConditions(
                conditions,
                context.getWorkflow().getEdges()
        );

        for (SwitchCondition switchCondition : conditions) {

            boolean matched =
                    conditionEvaluator.evaluateConditionGroups(
                            switchCondition.getFilterCondition(),
                            event.getTicketData(),
                            event.getOldTicketData()
                    );

            if (!matched) {
                continue;
            }

            WorkflowNode targetNode =
                    workflowNavigator.findNode(
                            context.getWorkflow(),
                            switchCondition.getId()
                    );

            if (targetNode == null) {

                log.warn(
                        "Cannot find target node. targetId={}",
                        switchCondition.getId()
                );

                continue;
            }

            log.info(
                    "Switch matched branch. currentNode={}, targetNode={}",
                    context.getCurrentNode().getId(),
                    targetNode.getId()
            );

            return ActionResult.success(
                    Map.of(
                            "nextNodeId", targetNode.getId(),
                            "branchType", "FILTER"
                    )
            );
        }

        log.info(
                "No switch condition matched. nodeId={}",
                context.getCurrentNode().getId()
        );

        return ActionResult.skipped();
    }

    private ActionResult handlePercentSwitch(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        List<WorkflowNode> children =
                workflowNavigator.nextNodes(
                        context.getWorkflow(),
                        context.getCurrentNode(),
                        ActionResult.success()
                );

        if (children.isEmpty()) {

            log.warn(
                    "No child node found for percent switch. nodeId={}",
                    context.getCurrentNode().getId()
            );

            return ActionResult.skipped();
        }

        WorkflowNode selectedNode =
                selectNodeWithLowestCount(children);

        if (selectedNode == null) {

            log.warn(
                    "Cannot select node for percent switch. nodeId={}",
                    context.getCurrentNode().getId()
            );

            return ActionResult.skipped();
        }

        increaseNodeCounter(
                selectedNode,
                event
        );

        log.info(
                "Percent switch selected node={}",
                selectedNode.getId()
        );

        return ActionResult.success(
                Map.of(
                        "nextNodeId", selectedNode.getId(),
                        "branchType", "PERCENT"
                )
        );
    }

    private WorkflowNode selectNodeWithLowestCount(
            List<WorkflowNode> nodes
    ) {

        return nodes.stream()
                .filter(node -> node.getData() != null)
                .min(
                        Comparator.comparingLong(
                                node -> safeCount(node.getData())
                        )
                )
                .orElse(null);
    }

    private long safeCount(
            WorkFlowNoteData nodeData
    ) {

        if (nodeData.getObjectCount() == null) {
            return 0L;
        }

        return nodeData.getObjectCount();
    }

    private void increaseNodeCounter(
            WorkflowNode node,
            WorkflowEvent event
    ) {

        if (event.isTestWorkflow()) {
            return;
        }

        WorkFlowNoteData nodeData = node.getData();

        Long current =
                nodeData.getObjectCount() == null
                        ? 0L
                        : nodeData.getObjectCount();

        nodeData.setObjectCount(current + 1);
    }
}