"""
src/agents/base_agent.py
===========================
BASE LANGGRAPH AGENT + all 5 agent class stubs.
CreditAnalysisAgent is the reference implementation with full LangGraph pattern.
The other 4 agents are stubs with complete docstrings for implementation.
"""
from __future__ import annotations
import asyncio, hashlib, json, time, os, logging
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4
from langgraph.graph import StateGraph, END

logger = logging.getLogger(__name__)


LANGGRAPH_VERSION = "1.0.0"
MAX_OCC_RETRIES = 5

class BaseApexAgent(ABC):
    """
    Base for all 5 Apex agents. Provides Gas Town session management,
    per-node event recording, tool call recording, OCC retry scaffolding.

    AGENT NODE SEQUENCE (all agents follow this):
        start_session → validate_inputs → load_context → [domain nodes] → write_output → end_session

    Each node must call self._record_node_execution() at its end.
    Each tool/registry call must call self._record_tool_call().
    The write_output node must call self._record_output_written() then self._record_node_execution().
    """
    def __init__(self, agent_id: str, agent_type: str, store, registry, client=None, model="google/gemini-2.5-pro", log_dir="logs"):
        self.agent_id = agent_id; self.agent_type = agent_type
        self.store = store; self.registry = registry; self.client = client; self.model = model
        self.session_id = None; self.application_id = None
        self._session_stream = None; self._t0 = None
        self._seq = 0; self._llm_calls = 0; self._tokens = 0; self._cost = 0.0
        self._graph = None
        self._llm_client = None
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"{self.agent_type}.log")
        self.jsonl_file = os.path.join(self.log_dir, f"{self.agent_type}_events.jsonl")

    @abstractmethod
    def build_graph(self): raise NotImplementedError

    async def process_application(self, application_id: str, recover_from_session_id: str = None) -> None:
        if not self._graph: self._graph = self.build_graph()
        self.application_id = application_id
        self.session_id = f"sess-{self.agent_type[:3]}-{uuid4().hex[:8]}"
        self._session_stream = f"agent-{self.agent_type}-{self.session_id}"
        self._t0 = time.time(); self._seq = 0; self._llm_calls = 0; self._tokens = 0; self._cost = 0.0
        
        self._recovered_nodes = set()
        if recover_from_session_id:
            prior_stream = f"agent-{self.agent_type}-{recover_from_session_id}"
            try:
                prior_events = await self.store.load_stream(prior_stream)
                for ev in prior_events:
                    if ev.event_type == "AgentNodeExecuted":
                        self._recovered_nodes.add(ev.payload["node_name"])
            except Exception as e:
                logger.warning(f"Could not load prior session for recovery: {e}")

        await self._start_session(application_id, recover_from_session_id)
        
        if recover_from_session_id:
            await self._append_session({"event_type": "AgentSessionRecovered", "event_version": 1, "payload": {
                "session_id": self.session_id,
                "agent_type": self.agent_type,
                "application_id": self.application_id,
                "recovered_from_session_id": recover_from_session_id,
                "recovery_point": list(self._recovered_nodes)[-1] if self._recovered_nodes else "start",
                "recovered_at": datetime.now().isoformat()
            }})
            
        try:
            result = await self._graph.ainvoke(self._initial_state(application_id))
            await self._complete_session(result)
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e)); raise

    def _initial_state(self, app_id):
        return {"application_id": app_id, "session_id": self.session_id,
                "agent_id": self.agent_id, "errors": [], "output_events_written": [], "next_agent_triggered": None}

    async def _start_session(self, app_id, recover_from_session_id=None):
        ctx_source = f"prior_session_replay:{recover_from_session_id}" if recover_from_session_id else "fresh"
        await self._append_session({"event_type":"AgentSessionStarted","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"agent_id":self.agent_id,
            "application_id":app_id,"model_version":self.model,"langgraph_graph_version":LANGGRAPH_VERSION,
            "context_source":ctx_source,"context_token_count":1000,"started_at":datetime.now().isoformat()}})

    async def _record_node_execution(self, name, in_keys, out_keys, ms, tok_in=None, tok_out=None, cost=None):
        if hasattr(self, "_recovered_nodes") and name in self._recovered_nodes:
            return  # Skip duplicate log for replayed nodes
            
        self._seq += 1
        if tok_in: self._tokens += tok_in + (tok_out or 0); self._llm_calls += 1
        if cost: self._cost += cost
        await self._append_session({"event_type":"AgentNodeExecuted","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"node_name":name,
            "node_sequence":self._seq,"input_keys":in_keys,"output_keys":out_keys,
            "llm_called":tok_in is not None,"llm_tokens_input":tok_in,"llm_tokens_output":tok_out,
            "llm_cost_usd":cost,"duration_ms":ms,"executed_at":datetime.now().isoformat()}})

    async def _record_tool_call(self, tool, inp, out, ms):
        await self._append_session({"event_type":"AgentToolCalled","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"tool_name":tool,
            "tool_input_summary":inp,"tool_output_summary":out,"tool_duration_ms":ms,
            "called_at":datetime.now().isoformat()}})

    async def _record_output_written(self, events_written, summary):
        await self._append_session({"event_type":"AgentOutputWritten","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"application_id":self.application_id,
            "events_written":events_written,"output_summary":summary,"written_at":datetime.now().isoformat()}})

    async def _record_input_validated(self, inputs_validated: list, ms: int):
        from src.models.events import AgentType
        await self._append_session({"event_type":"AgentInputValidated","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,
            "application_id":self.application_id,
            "inputs_validated":inputs_validated,
            "validation_duration_ms":ms,
            "validated_at":datetime.now().isoformat()}})

    async def _record_input_failed(self, missing_inputs: list, errors: list):
        from src.models.events import AgentType
        await self._append_session({"event_type":"AgentInputValidationFailed","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,
            "application_id":self.application_id,
            "missing_inputs":missing_inputs,
            "validation_errors":errors,
            "failed_at":datetime.now().isoformat()}})

    async def _complete_session(self, result):
        ms = int((time.time()-self._t0)*1000)
        await self._append_session({"event_type":"AgentSessionCompleted","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"application_id":self.application_id,
            "total_nodes_executed":self._seq,"total_llm_calls":self._llm_calls,"total_tokens_used":self._tokens,
            "total_cost_usd":round(self._cost,6),"total_duration_ms":ms,
            "next_agent_triggered":result.get("next_agent_triggered"),"completed_at":datetime.now().isoformat()}})

    async def _fail_session(self, etype, emsg):
        await self._append_session({"event_type":"AgentSessionFailed","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"application_id":self.application_id,
            "error_type":etype,"error_message":emsg[:500],"last_successful_node":f"node_{self._seq}",
            "recoverable":etype in ("llm_timeout","RateLimitError"),"failed_at":datetime.now().isoformat()}})

    async def _append_session(self, event: dict):
        """Append agent internal session logs to the event store, log file, and JSONL file."""
        # 1. Standard Event Store write
        try:
            ver = await self.store.stream_version(self._session_stream)
            await self.store.append(
                stream_id=self._session_stream,
                events=[event],
                expected_version=ver,
                causation_id=self.session_id
            )
        except Exception as e:
            logger.error(f"Failed to append session log to store: {e}")
            print(f"  [{self.agent_type[:8]}:{self.session_id}] {event['event_type']} (Store Error: {e})")

        # 2. File Logging (Human readable)
        timestamp = datetime.now().isoformat()
        log_entry = f"[{timestamp}] [{self.session_id}] {event['event_type']}: {json.dumps(event['payload'], default=str)}\n"
        with open(self.log_file, "a") as f:
            f.write(log_entry)

        # 3. JSONL Logging (Machine readable)
        with open(self.jsonl_file, "a") as f:
            f.write(json.dumps({"timestamp": timestamp, "session_id": self.session_id, **event}, default=str) + "\n")

    async def _append_stream(self, stream_id: str, event_dict: dict, causation_id: str = None):
        """Append to any aggregate stream with OCC retry."""
        for attempt in range(MAX_OCC_RETRIES):
            try:
                ver = await self.store.stream_version(stream_id)
                await self.store.append(stream_id=stream_id, events=[event_dict],
                    expected_version=ver, causation_id=causation_id)
                return
            except Exception as e:
                if "OptimisticConcurrencyError" in type(e).__name__ and attempt < MAX_OCC_RETRIES-1:
                    await asyncio.sleep(0.1 * (2**attempt)); continue
                raise

    async def _append_with_retry(self, stream_id: str, events: list[dict], causation_id: str = None) -> list[int]:
        """Append multiple events to any aggregate stream with OCC retry. Returns stream positions."""
        positions = []
        for attempt in range(MAX_OCC_RETRIES):
            try:
                ver = await self.store.stream_version(stream_id)
                await self.store.append(
                    stream_id=stream_id,
                    events=events,
                    expected_version=ver,
                    causation_id=causation_id
                )
                return [ver + i for i in range(len(events))]
            except Exception as e:
                if "OptimisticConcurrencyError" in type(e).__name__ and attempt < MAX_OCC_RETRIES-1:
                    await asyncio.sleep(0.1 * (2**attempt)); continue
                raise
        return positions

    async def _call_llm(self, system, user, max_tokens=1024):
        if self._llm_client is None:
            from openai import AsyncOpenAI
            api_key = os.environ.get("OPENROUTER_API_KEY", "dummy_key")
            self._llm_client = AsyncOpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=api_key
            )

        resp = await self._llm_client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ],
            max_tokens=max_tokens,
            stream=False,
        )

        text = resp.choices[0].message.content or ""
        usage = resp.usage
        i = usage.prompt_tokens if usage else 0
        o = usage.completion_tokens if usage else 0
        return text, int(i), int(o), 0.0

    @staticmethod
    def _sha(d): return hashlib.sha256(json.dumps(str(d),sort_keys=True).encode()).hexdigest()[:16]

    @staticmethod
    def _parse_json(content: str) -> dict:
        import re
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                return json.loads(match.group())
            return {}



# Agent class templates removed - see dedicated files in src/agents/*.py
# This file now only contains the BaseApexAgent foundation.
