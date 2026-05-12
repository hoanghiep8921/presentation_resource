package com.vnpay.workflow_engine.service.actions;

import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.workflow_engine.constants.Constant;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.engine.ActionResult;
import com.vnpay.workflow_engine.engine.ConditionEvaluatorService;
import com.vnpay.workflow_engine.service.ConditionActionHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SendEmailActionHandler
        extends ConditionActionHandler {

    public SendEmailActionHandler(
            ConditionEvaluatorService evaluator
    ) {
        super(evaluator);
    }

    @Override
    public String getActionType() {
        return Constant.SEND_EMAIL;
    }

    @Override
    protected ActionResult doExecute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        log.info(
                "Send email for ticket={}",
                event.getTicketData().get("id")
        );

        // TODO send email

        return ActionResult.success();
    }
}