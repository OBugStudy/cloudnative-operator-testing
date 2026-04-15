import os
from typing import Tuple

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


def _call_llm_for_branch_flip(prompt: str, max_tokens: int = 4096) -> Tuple[str, str]:
    """调用 DeepSeek LLM，返回 (action, cr_yaml_or_error)。"""

    api_key = os.getenv("API_KEY")
    if not api_key:
        return "error", "API_KEY not set"

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": "You are a Kubernetes CR mutation expert.",
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
            stream=False,
        )
        result = resp.choices[0].message.content.strip()

        for fence in ("```json", "```yaml", "```"):
            if result.startswith(fence):
                result = result[len(fence) :]
        if result.endswith("```"):
            result = result[:-3]
        return "update", result.strip()
    except Exception as e:
        return "error", str(e)


def _call_llm_test_plan(prompt: str) -> tuple:
    """Call LLM to get a test plan. Returns (raw_response_str, error_str)."""
    try:
        action, content = _call_llm_for_branch_flip(prompt)
        if action == "error":
            return "", f"LLM error: {content}"
        return content or "", ""
    except Exception as e:
        return "", str(e)