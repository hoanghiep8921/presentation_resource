package com.vnpay.workflow_engine.engine;

import lombok.Builder;

import java.util.Map;

@Builder
public record ActionResult(
        ActionStatus status,
        Map<String, Object> data,
        Throwable error
) {

    public static final String NEXT_NODE_ID = "nextNodeId";

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

    /**
     * Dùng cho DELAY: branch tạm dừng, sẽ được resume bởi Kafka self-loop.
     */
    public static ActionResult waiting(Map<String, Object> data) {
        return ActionResult.builder()
                .status(ActionStatus.WAITING)
                .data(data == null ? Map.of() : data)
                .build();
    }

    public boolean isSuccess() {
        return status == ActionStatus.SUCCESS;
    }

    public boolean isFailed() {
        return status == ActionStatus.FAILED;
    }

    public boolean isSkipped() {
        return status == ActionStatus.SKIPPED;
    }

    public boolean isWaiting() {
        return status == ActionStatus.WAITING;
    }

    /**
     * Trả về nodeId mà handler tường minh chỉ định cho branch tiếp theo
     * (ví dụ Switch khi match được 1 nhánh). Navigator sẽ ưu tiên cái này
     * thay vì duyệt tất cả các edge ra.
     */
    public String nextNodeId() {
        if (data == null) {
            return null;
        }
        Object value = data.get(NEXT_NODE_ID);
        return value == null ? null : value.toString();
    }
}
