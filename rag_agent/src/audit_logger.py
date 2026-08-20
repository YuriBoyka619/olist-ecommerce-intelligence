import json
import uuid
from pathlib import Path
from datetime import datetime, timezone


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "agent_audit.jsonl"

LOG_DIR.mkdir(exist_ok=True)


def write_audit_log(
    question,
    plan=None,
    tool_results=None,
    status="success",
    error=None
):
    request_id = str(uuid.uuid4())

    selected_tools = []

    if plan:
        selected_tools = [
            call.get("tool")
            for call in plan
        ]

    tool_execution = []

    if tool_results:
        for result in tool_results:
            tool_execution.append({
                "tool": result.get("tool"),
                "status": result.get("status"),
                "error": result.get("error")
            })

    record = {
        "request_id": request_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "question": question,
        "selected_tools": selected_tools,
        "tool_execution": tool_execution,
        "status": status,
        "error": error
    }

    with open(LOG_FILE, "a", encoding="utf-8") as file:
        file.write(
            json.dumps(
                record,
                ensure_ascii=False
            )
            + "\n"
        )

    return request_id