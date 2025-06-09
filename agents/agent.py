from typing import Any, Generator, Optional
from uuid import uuid4

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.entities import SpanType
from mlflow.pyfunc.model import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

mlflow.openai.autolog()
LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"

class CustomChatAgent(ChatAgent):
    def __init__(self):
        self.workspace_client = WorkspaceClient()
        self.client = self.workspace_client.serving_endpoints.get_open_ai_client()
        self.llm_endpoint = LLM_ENDPOINT_NAME

    def prepare_messages_for_llm(self, messages: list[ChatAgentMessage]) -> list[dict[str, Any]]:
        """Filter out ChatAgentMessage fields that are not compatible with LLM message formats"""
        compatible_keys = ["role", "content", "name", "tool_calls", "tool_call_id"]
        return [
            {k: v for k, v in m.model_dump_compat(exclude_none=True).items() if k in compatible_keys} for m in messages
        ]

    #@mlflow.trace(span_type=SpanType.AGENT)
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None
    ) -> ChatAgentResponse:
        fixed_prompt = "You're and AI assistant, users will provide the process deviation or process SOP related paragraphs from those Identify the most important keywords  and generate a concise, 10-12 word title that includes keywords.that includes them without adding additional notes or explanations Ensure to include any of the following keywords if they appear: SOP, TM-, KG, GMP (maintain original capitalization).  Adopt a formal, business-appropriate tone, Additionally, respond politely to users if they greet you, but focus on delivering high-quality service. Provide only the top 4 suggestions."

        messages.insert(0, ChatAgentMessage(role="system", content=fixed_prompt))

        resp = self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=self.prepare_messages_for_llm(messages),
        )

        return ChatAgentResponse(
            messages=[ChatAgentMessage(**resp.choices[0].message.to_dict(), id=str(uuid4()))],
        )


from mlflow.models import set_model

AGENT = CustomChatAgent()
set_model(AGENT)
