package com.vnpay.workflow_engine.service.actions;

import com.vnpay.core.dto.event.DelayEvent;
import com.vnpay.core.dto.event.WorkflowEvent;
import com.vnpay.core.entity.workflows.WorkFlowNoteData;
import com.vnpay.core.entity.workflows.WorkflowNode;
import com.vnpay.core.utils.StringUtil;
import com.vnpay.core.utils.TimeUtil;
import com.vnpay.workflow_engine.constants.Constant;
import com.vnpay.workflow_engine.dto.WorkflowContext;
import com.vnpay.workflow_engine.engine.ActionResult;
import com.vnpay.workflow_engine.engine.ConditionEvaluatorService;
import com.vnpay.workflow_engine.engine.KafkaService;
import com.vnpay.workflow_engine.service.ConditionActionHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;

@Component
@Slf4j
public class DelayActionHandler extends ConditionActionHandler {

    @Value("${kafka.topic.delay-workflow-topic:ici.workflow-engine.delay}")
    private String delayWorkflowTopic;

    private final KafkaService kafkaService;

    public DelayActionHandler(
            ConditionEvaluatorService evaluator,
            KafkaService kafkaService
    ) {
        super(evaluator);
        this.kafkaService = kafkaService;
    }

    @Override
    public String getActionType() {
        return Constant.DELAY;
    }

    @Override
    protected ActionResult doExecute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        WorkflowNode currentNode =
                context.getCurrentNode();

        WorkFlowNoteData nodeData =
                currentNode.getData();

        log.info(
                "Execute delay action. workflowId={}, nodeId={}, subType={}",
                context.getWorkflow().getId(),
                currentNode.getId(),
                nodeData.getSubActionType()
        );

        LocalDateTime scheduledAt =
                calculateDelayTime(
                        nodeData,
                        event.getTicketData()
                );

        if (scheduledAt == null) {

            log.warn(
                    "Cannot calculate delay time. nodeId={}",
                    currentNode.getId()
            );

            return ActionResult.skipped();
        }

        log.info(
                "Delay calculated. workflowId={}, nodeId={}, scheduledAt={}",
                context.getWorkflow().getId(),
                currentNode.getId(),
                scheduledAt
        );

        Map<String, Object> payload = Map.of(
                "scheduledAt", scheduledAt,
                "delayType", nodeData.getSubActionType()
        );

        // Test mode (doc D.3): bỏ qua wait, đi tiếp ngay để portal preview timeline.
        if (event.isTestWorkflow()) {
            return ActionResult.success(payload);
        }

        // Production: publish Kafka self-loop và PAUSE branch.
        // Khi đến giờ, WorkflowEventConsumer.listenDelayEvent sẽ resume
        // và gọi lại executor để chạy tiếp node sau DELAY.
        publishDelayEvent(context, event, scheduledAt);

        return ActionResult.waiting(payload);
    }

    private void publishDelayEvent(
            WorkflowContext context,
            WorkflowEvent event,
            LocalDateTime scheduledAt
    ) {

        String ticketId =
                (String) event.getTicketData()
                        .getOrDefault("id", "");

        DelayEvent delayEvent =
                DelayEvent.builder()
                        .id(context.getWorkflow().getId())
                        .ticketId(ticketId)
                        .scheduledAt(scheduledAt)
                        .currentNodeId(
                                context.getCurrentNode().getId()
                        )
                        .data(event.getTicketData())
                        .action(event.getAction())
                        .build();

        kafkaService.sendMessage(
                delayWorkflowTopic,
                delayEvent
        );

        log.info(
                "Delay event published. workflowId={}, ticketId={}, scheduledAt={}",
                context.getWorkflow().getId(),
                ticketId,
                scheduledAt
        );
    }

    private LocalDateTime calculateDelayTime(
            WorkFlowNoteData nodeData,
            Map<String, Object> payload
    ) {

        String subActionType =
                nodeData.getSubActionType();

        if (Constant.TIME.equals(subActionType)) {
            return calculateFixedTimeDelay(nodeData);
        }

        if (Constant.EVENT.equals(subActionType)) {
            return calculateEventBasedDelay(
                    nodeData,
                    payload
            );
        }

        log.warn(
                "Unsupported delay subActionType={}",
                subActionType
        );

        return null;
    }

    private LocalDateTime calculateFixedTimeDelay(
            WorkFlowNoteData nodeData
    ) {

        String delayTime =
                (String) nodeData.getAdditionalInfo()
                        .get(Constant.AdditionalInfoKey.DAY_TIME);

        if (StringUtil.isNullOrEmpty(delayTime)) {

            log.warn("Delay time is empty");

            return null;
        }

        LocalTime localTime =
                LocalTime.parse(delayTime);

        LocalDateTime now =
                LocalDateTime.now();

        LocalDateTime scheduledTime =
                LocalDateTime.of(
                        LocalDate.now(),
                        localTime
                );

        if (scheduledTime.isBefore(now)) {
            scheduledTime = scheduledTime.plusDays(1);
        }

        return scheduledTime;
    }

    private LocalDateTime calculateEventBasedDelay(
            WorkFlowNoteData nodeData,
            Map<String, Object> payload
    ) {

        String fieldExpression =
                (String) nodeData.getAdditionalInfo()
                        .get(Constant.AdditionalInfoKey.VALUE);

        Object actualValue =
                StringUtil.getActualValue(
                        fieldExpression,
                        payload
                );

        LocalDateTime baseTime =
                TimeUtil.toLocalDateTime(actualValue);

        if (baseTime == null) {

            log.warn(
                    "Cannot parse baseTime from payload. expression={}",
                    fieldExpression
            );

            return null;
        }

        double offset =
                ((Number) nodeData.getAdditionalInfo()
                        .getOrDefault("offset_number", 0D))
                        .doubleValue();

        String offsetUnit =
                (String) nodeData.getAdditionalInfo()
                        .getOrDefault("offset_unit", "hour");

        return TimeUtil.addOffset(
                baseTime,
                offset,
                offsetUnit
        );
    }
}