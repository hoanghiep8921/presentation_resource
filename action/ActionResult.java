package com.vnpay.workflow_engine.engine;

import lombok.Builder;

import java.util.Map;

@Builder
public record ActionResult(
        ActionStatus status,
        Map<String, Object> data,
        Throwable error
) {

    public static ActionResult success() {
        return ActionResult.builder()
                .status(ActionStatus.SUCCESS)
                .data(Map.of())
                .build();
    }

    public static ActionResult success(Map<String, Object> data) {
        return ActionResult.builder()
                .status(ActionStatus.SUCCESS)
                .data(data)
                .build();
    }

    public static ActionResult failed(Throwable throwable) {
        return ActionResult.builder()
                .status(ActionStatus.FAILED)
                .error(throwable)
                .data(Map.of())
                .build();
    }

    public static ActionResult skipped() {
        return ActionResult.builder()
                .status(ActionStatus.SKIPPED)
                .data(Map.of())
                .build();
    }

    public boolean isSuccess() {
        return status == ActionStatus.SUCCESS;
    }

    public boolean isFailed() {
        return status == ActionStatus.FAILED;
    }
}