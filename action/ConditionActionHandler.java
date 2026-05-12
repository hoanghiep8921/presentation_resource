package com.vnpay.workflow_engine.service;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.engine.ConditionEvaluatorService;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class ConditionActionHandler
        extends AbstractActionHandler {

    protected final ConditionEvaluatorService evaluator;

    @Override
    protected boolean canExecute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        return evaluator.evaluateConditionGroups(
                context.getCurrentNode()
                        .getData()
                        .getFilterCondition(),
                event.getTicketData(),
                event.getOldTicketData()
        );
    }
}