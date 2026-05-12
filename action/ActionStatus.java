package com.vnpay.workflow_engine.engine;

public enum ActionStatus {

    /**
     * Action thực thi xong, đi tiếp theo edge mặc định.
     */
    SUCCESS,

    /**
     * Action lỗi → đi tiếp theo edge `red` (error path).
     */
    FAILED,

    /**
     * Filter / switch không match → dừng branch hiện tại.
     */
    SKIPPED,

    /**
     * Action chủ động pause pipeline (vd DELAY) → branch sẽ được resume
     * bởi cơ chế khác (Kafka self-loop), KHÔNG đi tiếp ngay.
     */
    WAITING
}
