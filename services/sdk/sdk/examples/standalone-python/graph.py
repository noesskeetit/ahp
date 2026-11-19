"""Optional LangGraph builder for ahp-agent-container, independent of Agent Space."""
from langgraph.graph import START, END, StateGraph


def build(ctx):
    graph = StateGraph(dict)
    graph.add_node("increment", lambda state: {"count": state.get("count", 0) + 1})
    graph.add_edge(START, "increment")
    graph.add_edge("increment", END)
    return graph.compile(checkpointer=ctx.saver, store=ctx.store)
