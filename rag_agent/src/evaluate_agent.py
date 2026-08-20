from agent import create_plan


TEST_CASES = [
    {
        "question": "How many orders are in each order status?",
        "expected_tools": {"order_status_summary"},
    },
    {
        "question": "What are customers complaining about regarding late delivery?",
        "expected_tools": {"rag"},
    },
    {
        "question": "Show me the 5 orders with the highest predicted late-delivery risk.",
        "expected_tools": {"high_risk_orders"},
    },
    {
        "question": "What is our delivery performance, and what are customers saying about late deliveries?",
        "expected_tools": {"delivery_performance", "rag"},
    },
    {
        "question": (
            "Which orders are at highest late-delivery risk, "
            "and how does our historical delivery performance compare "
            "with what customers are saying about late deliveries?"
        ),
        "expected_tools": {
            "high_risk_orders",
            "delivery_performance",
            "rag",
        },
    },
]


passed = 0

for index, test in enumerate(TEST_CASES, start=1):

    print(f"\nTest {index}")
    print("Question:", test["question"])

    try:
        plan = create_plan(test["question"])

        actual_tools = {
            call.get("tool")
            for call in plan
        }

        expected_tools = test["expected_tools"]

        print("Expected:", expected_tools)
        print("Actual:  ", actual_tools)

        if actual_tools == expected_tools:
            print("PASS ✅")
            passed += 1
        else:
            print("FAIL ❌")

    except Exception as exc:
        print("ERROR ❌")
        print(str(exc))


print("\n==============================")
print(f"Passed: {passed}/{len(TEST_CASES)}")
print("==============================")