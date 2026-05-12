public abstract class AbstractActionHandler {

    public abstract String getActionType();

    public final ActionResult execute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        if (!canExecute(context, event)) {
            return ActionResult.skipped();
        }

        try {
            return doExecute(context, event);
        } catch (Exception ex) {
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
