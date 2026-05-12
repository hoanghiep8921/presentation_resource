@Service
@RequiredArgsConstructor
public class WorkflowExecutor {

    private final ActionHandlerFactory handlerFactory;
    private final WorkflowNavigator navigator;
    private final WorkflowHistoryService historyService;

    public void execute(
            WorkflowContext context,
            WorkflowEvent event
    ) {

        Deque<WorkflowNode> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();

        queue.add(context.getCurrentNode());

        while (!queue.isEmpty()) {

            WorkflowNode node = queue.poll();

            if (!visited.add(node.getId())) {
                continue;
            }

            processNode(context, node, event, queue);
        }
    }

    private void processNode(
            WorkflowContext context,
            WorkflowNode node,
            WorkflowEvent event,
            Queue<WorkflowNode> queue
    ) {

        if (navigator.isEndNode(node)) {
            historyService.completeWorkflow(context, event);
            return;
        }

        AbstractActionHandler handler =
                handlerFactory.getHandler(
                        node.getData().getActionType()
                );

        ActionResult result =
                handler.execute(
                        new WorkflowContext(
                                context.getWorkflow(),
                                node
                        ),
                        event
                );

        historyService.save(node, result, event);

        List<WorkflowNode> nextNodes =
                navigator.nextNodes(
                        context.getWorkflow(),
                        node,
                        result
                );

        queue.addAll(nextNodes);
    }
}
