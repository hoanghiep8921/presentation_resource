package com.vnpay.workflow_engine.service;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.engine.ActionResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractActionHandler {

    public abstract String getActionType();

    public final ActionResult execute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        try {

            if (!canExecute(context, event)) {
                log.info("Skip node {}", context.getCurrentNode().getId());
                return ActionResult.skipped();
            }

            return doExecute(context, event);

        } catch (Exception ex) {

            log.error(
                    "Execute action error. workflowId={}, nodeId={}",
                    context.getWorkflow().getId(),
                    context.getCurrentNode().getId(),
                    ex
            );

            return ActionResult.failed(ex);
        }
    }

    protected boolean canExecute(
            WorkflowContext context,
            WorkflowEvent event
    ) {
        return true;
    }

    protected abstract ActionResult doExecute(
            WorkflowContext context,
            WorkflowEvent event
    );
}