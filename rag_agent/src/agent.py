import json

from google import genai

from audit_logger import write_audit_log

from rag_answer import answer_question, MODEL_NAME

from analytics_tool import (
    get_order_status_summary,
    get_delivery_performance,
    get_sales_summary,
    get_category_performance,
    get_seller_performance,
    get_customer_review_summary,
)

from ml_tool import (
    get_high_risk_orders,
    get_order_prediction,
    get_prediction_summary,
)


client = genai.Client()

MAX_TOOL_CALLS = 3


# ---------------------------------------------------------
# Approved tools available to the agent
# ---------------------------------------------------------
TOOL_REGISTRY = {
    "rag": answer_question,

    "order_status_summary": get_order_status_summary,
    "delivery_performance": get_delivery_performance,
    "sales_summary": get_sales_summary,
    "category_performance": get_category_performance,
    "seller_performance": get_seller_performance,
    "customer_review_summary": get_customer_review_summary,

    "high_risk_orders": get_high_risk_orders,
    "order_prediction": get_order_prediction,
    "prediction_summary": get_prediction_summary,
}


# ---------------------------------------------------------
# Ask Gemini which approved tool(s) should be used
# ---------------------------------------------------------
def create_plan(question):
    prompt = f"""
You are the routing layer for the Olist Operations Assistant.

Your job is ONLY to decide which approved tools should answer the user's question.

AVAILABLE TOOLS:

1. rag
Use for:
- customer review text
- complaints
- opinions
- qualitative feedback
- what customers are saying

Arguments:
{{
    "question": "original user question",
    "top_k": 5
}}

2. order_status_summary
Use for:
- counts by order status
- delivered, shipped, canceled, unavailable orders

Arguments:
{{}}

3. delivery_performance
Use for:
- historical delivery KPIs
- delivered orders
- late orders
- average delivery duration

Arguments:
{{}}

4. sales_summary
Use for:
- overall sales/value KPIs
- total orders
- item value
- freight value

Arguments:
{{}}

5. category_performance
Use for:
- top product categories
- category performance

Arguments:
{{
    "limit": 10
}}

6. seller_performance
Use for:
- top sellers
- seller performance

Arguments:
{{
    "limit": 10
}}

7. customer_review_summary
Use for:
- review counts
- average review score
- positive/negative review counts

Arguments:
{{}}

8. high_risk_orders
Use for:
- orders with highest predicted late-delivery risk

Arguments:
{{
    "limit": 10
}}

9. order_prediction
Use when the user provides one specific 32-character Olist order_id.

Arguments:
{{
    "order_id": "..."
}}

10. prediction_summary
Use for:
- overall ML prediction statistics
- how many orders are predicted late
- high/medium/low risk counts

Arguments:
{{}}

ROUTING RULES:

- Historical facts and KPIs -> analytics tools.
- Future/predicted delivery risk -> ML tools.
- Customer review meaning/complaints -> RAG.
- A question may require multiple tools.
- Never create SQL.
- Never invent another tool.
- Maximum {MAX_TOOL_CALLS} tool calls.
- Use the minimum number of tools necessary.

Return ONLY valid JSON.

Required JSON format:

{{
    "calls": [
        {{
            "tool": "tool_name",
            "arguments": {{}}
        }}
    ]
}}

USER QUESTION:
{question}
"""

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt
    )

    text = response.text.strip()

    # Handle accidental Markdown code fences
    if text.startswith("```"):
        text = text.replace("```json", "", 1)
        text = text.replace("```", "")
        text = text.strip()

    plan = json.loads(text)

    calls = plan.get("calls", [])

    if not isinstance(calls, list):
        raise ValueError("Agent plan must contain a list of calls.")

    return calls[:MAX_TOOL_CALLS]


# ---------------------------------------------------------
# Safely execute approved tool calls
# ---------------------------------------------------------
def execute_tool(tool_name, arguments, original_question):
    if tool_name not in TOOL_REGISTRY:
        raise ValueError(f"Tool not allowed: {tool_name}")

    arguments = arguments or {}

    if tool_name == "rag":
        return answer_question(
            arguments.get("question", original_question),
            top_k=int(arguments.get("top_k", 5))
        )

    if tool_name == "category_performance":
        return get_category_performance(
            int(arguments.get("limit", 10))
        )

    if tool_name == "seller_performance":
        return get_seller_performance(
            int(arguments.get("limit", 10))
        )

    if tool_name == "high_risk_orders":
        return get_high_risk_orders(
            int(arguments.get("limit", 10))
        )

    if tool_name == "order_prediction":
        order_id = arguments.get("order_id")

        if not order_id:
            raise ValueError(
                "order_prediction requires order_id."
            )

        return get_order_prediction(order_id)

    # No-argument tools
    return TOOL_REGISTRY[tool_name]()


# ---------------------------------------------------------
# Execute complete plan
# ---------------------------------------------------------
def run_tools(question, calls):
    results = []

    for call in calls:
        tool_name = call.get("tool")
        arguments = call.get("arguments", {})

        try:
            output = execute_tool(
                tool_name,
                arguments,
                question
            )

            results.append({
                "tool": tool_name,
                "status": "success",
                "output": output
            })

        except Exception as exc:
            results.append({
                "tool": tool_name,
                "status": "error",
                "error": str(exc)
            })

    return results


# ---------------------------------------------------------
# Generate final grounded answer
# ---------------------------------------------------------
def synthesize_answer(question, tool_results):
    serialized_results = json.dumps(
        tool_results,
        indent=2,
        ensure_ascii=False,
        default=str
    )

    prompt = f"""
You are the Olist Operations Assistant.

Answer the user's question using ONLY the tool results below.

Rules:
- Do not invent numbers or facts.
- Do not claim anything not supported by tool results.
- Clearly distinguish historical analytics from ML predictions.
- ML probabilities are predictions, not guaranteed outcomes.
- If RAG evidence is used, describe it as customer-review evidence.
- If a tool failed, mention the limitation only if relevant.
- Be concise and business-friendly.

USER QUESTION:
{question}

TOOL RESULTS:
{serialized_results}

FINAL ANSWER:
"""

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt
    )

    return response.text

def get_friendly_error_message(error):
    error_text = str(error).lower()

    if "429" in error_text or "resource_exhausted" in error_text:
        return (
            "The AI model has reached its current API quota. "
            "Please try again later or use another available API key/project."
        )

    if (
        "interactive authentication is required" in error_text
        or "devicecodecredential" in error_text
    ):
        return (
            "Microsoft Fabric authentication is required. "
            "Please complete the device-code sign-in and try again."
        )

    if "no api key was provided" in error_text:
        return (
            "The Gemini API key is not configured correctly. "
            "Please check the GEMINI_API_KEY environment variable."
        )

    if "invalid order_id" in error_text:
        return "The supplied Olist order ID is not valid."

    if "table not allowed" in error_text:
        return (
            "The requested data source is blocked by the agent's "
            "security guardrails."
        )

    return (
        "The agent encountered an unexpected error while processing "
        "your request. Please try again."
    )
# ---------------------------------------------------------
# Main agent
# ---------------------------------------------------------
def ask_agent(question):
    plan = []
    tool_results = []

    try:
        plan = create_plan(question)

        if not plan:
            write_audit_log(
                question=question,
                plan=[],
                tool_results=[],
                status="no_tool_selected"
            )

            return (
                "I could not determine an appropriate "
                "approved tool for this question."
            )

        print("\nAgent plan:")
        print(json.dumps(plan, indent=2))

        tool_results = run_tools(
            question,
            plan
        )

        # If every selected tool failed
        failed_tools = [
            result
            for result in tool_results
            if result.get("status") == "error"
        ]

        if tool_results and len(failed_tools) == len(tool_results):
            error_text = failed_tools[0].get(
                "error",
                "Unknown tool error"
            )

            request_id = write_audit_log(
                question=question,
                plan=plan,
                tool_results=tool_results,
                status="tool_error",
                error=error_text
            )

            print(f"\nAudit ID: {request_id}")

            return get_friendly_error_message(
                Exception(error_text)
            )

        answer = synthesize_answer(
            question,
            tool_results
        )

        request_id = write_audit_log(
            question=question,
            plan=plan,
            tool_results=tool_results,
            status="success"
        )

        print(f"\nAudit ID: {request_id}")

        return answer

    except Exception as exc:
        request_id = write_audit_log(
            question=question,
            plan=plan,
            tool_results=tool_results,
            status="error",
            error=str(exc)
        )

        print(f"\nAudit ID: {request_id}")

        return get_friendly_error_message(exc)


# ---------------------------------------------------------
# Interactive test
# ---------------------------------------------------------
if __name__ == "__main__":

    print("\nOlist Operations Assistant")
    print("Type 'exit' to stop.\n")

    while True:
        question = input("You: ").strip()

        if question.lower() in {
            "exit",
            "quit"
        }:
            break

        if not question:
            continue

        try:
            answer = ask_agent(question)

            print("\nAssistant:")
            print(answer)
            print()

        except Exception as exc:
            print(f"\nAgent error: {exc}\n")