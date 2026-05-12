package com.vnpay.workflow_engine.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ActionHandlerFactory {

    private final List<AbstractActionHandler> handlers;

    private final Map<String, AbstractActionHandler> handlerMap =
            new HashMap<>();

    @PostConstruct
    public void init() {

        handlers.forEach(handler ->
                handlerMap.put(
                        handler.getActionType(),
                        handler
                ));
    }

    public AbstractActionHandler getHandler(
            String actionType
    ) {

        AbstractActionHandler handler =
                handlerMap.get(actionType);

        if (handler == null) {
            throw new IllegalArgumentException(
                    "No handler found for actionType="
                            + actionType
            );
        }

        return handler;
    }
}