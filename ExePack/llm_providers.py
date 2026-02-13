# -*- coding: utf-8 -*-
"""
多模型/多平台 LLM 适配器（策略模式 + 工厂）
支持热切换：默认模型处理简单任务，强力模型处理深度报告。
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generator
import os

# 平台与模型配置（可扩展）
PLATFORM_MODELS: Dict[str, List[str]] = {
    "OpenAI": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-4", "gpt-3.5-turbo"],
    "DeepSeek": ["deepseek-chat", "deepseek-reasoner", "deepseek-coder"],
    "Claude": ["claude-3-5-sonnet-20241022", "claude-3-opus-20240229", "claude-3-haiku-20240307"],
    "Ollama": ["llama3.2", "qwen2.5", "deepseek-r1", "mistral"],
}

DEFAULT_BASE_URLS: Dict[str, str] = {
    "OpenAI": "https://api.openai.com/v1",
    "DeepSeek": "https://api.deepseek.com",
    "Claude": "https://api.anthropic.com",  # Anthropic 需单独 SDK，此处可用兼容端点
    "Ollama": "http://localhost:11434/v1",
}


class LLMProvider(ABC):
    """LLM 抽象基类：统一 chat 与 stream 接口"""

    @abstractmethod
    def chat(
        self,
        messages: List[Dict[str, str]],
        stream: bool = False,
    ):
        """
        非流式返回 content 字符串；流式返回 Generator[str, None, None]。
        """
        pass

    @abstractmethod
    def list_models(self) -> List[str]:
        """返回该平台可用模型列表（用于 UI 下拉）"""
        pass

    def test_connection(self) -> bool:
        """测试 API 连通性，子类可覆盖"""
        try:
            self.chat([{"role": "user", "content": "Hi"}], stream=False)
            return True
        except Exception:
            return False


class OpenAICompatibleProvider(LLMProvider):
    """OpenAI / DeepSeek 等兼容 OpenAI API 的提供商"""

    def __init__(self, api_key: str, base_url: str, model: str):
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        if not self._base_url.endswith("/v1"):
            self._base_url = self._base_url + "/v1" if "v1" not in self._base_url else self._base_url
        self._model = model

    def _client(self):
        from openai import OpenAI
        return OpenAI(api_key=self._api_key, base_url=self._base_url)

    def chat(self, messages: List[Dict[str, str]], stream: bool = False):
        client = self._client()
        resp = client.chat.completions.create(model=self._model, messages=messages, stream=stream)
        if stream:
            def gen():
                for chunk in resp:
                    if chunk.choices and chunk.choices[0].delta.content:
                        yield chunk.choices[0].delta.content
            return gen()
        return resp.choices[0].message.content or ""

    def list_models(self) -> List[str]:
        return PLATFORM_MODELS.get("OpenAI", []) + PLATFORM_MODELS.get("DeepSeek", [])


class DeepSeekProvider(OpenAICompatibleProvider):
    """DeepSeek 使用 OpenAI 兼容 API，仅 base_url 不同"""

    def __init__(self, api_key: str, base_url: Optional[str], model: str):
        base_url = base_url or DEFAULT_BASE_URLS["DeepSeek"]
        super().__init__(api_key, base_url, model)

    def list_models(self) -> List[str]:
        return PLATFORM_MODELS["DeepSeek"]


class ClaudeProvider(LLMProvider):
    """Claude（Anthropic）可走 OpenAI 兼容网关，此处用 OpenAI 兼容实现"""

    def __init__(self, api_key: str, base_url: Optional[str], model: str):
        self._api_key = api_key
        self._base_url = (base_url or "https://api.anthropic.com").rstrip("/")
        self._model = model

    def chat(self, messages: List[Dict[str, str]], stream: bool = False):
        # 若使用 OpenAI 兼容的 Claude 网关（如 one-api），可复用 OpenAICompatibleProvider
        from openai import OpenAI
        client = OpenAI(api_key=self._api_key, base_url=self._base_url)
        resp = client.chat.completions.create(model=self._model, messages=messages, stream=stream)
        if stream:
            def gen():
                for chunk in resp:
                    if chunk.choices and chunk.choices[0].delta.content:
                        yield chunk.choices[0].delta.content
                return
            return gen()
        return resp.choices[0].message.content or ""

    def list_models(self) -> List[str]:
        return PLATFORM_MODELS["Claude"]


class OllamaProvider(LLMProvider):
    """本地 Ollama，通常无需 API Key"""

    def __init__(self, api_key: str, base_url: Optional[str], model: str):
        self._base_url = (base_url or DEFAULT_BASE_URLS["Ollama"]).rstrip("/")
        self._model = model

    def chat(self, messages: List[Dict[str, str]], stream: bool = False):
        from openai import OpenAI
        client = OpenAI(api_key="ollama", base_url=self._base_url)
        resp = client.chat.completions.create(model=self._model, messages=messages, stream=stream)
        if stream:
            def gen():
                for chunk in resp:
                    if chunk.choices and chunk.choices[0].delta.content:
                        yield chunk.choices[0].delta.content
                return
            return gen()
        return resp.choices[0].message.content or ""

    def list_models(self) -> List[str]:
        return PLATFORM_MODELS["Ollama"]


def get_provider(
    platform: str,
    model: str,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Optional[LLMProvider]:
    """
    工厂方法：根据平台与模型返回对应 Provider。
    API Key 优先从参数获取，其次从环境变量 OPENAI_API_KEY / DEEPSEEK_API_KEY 等读取。
    """
    api_key = api_key or os.environ.get("OPENAI_API_KEY") or os.environ.get("DEEPSEEK_API_KEY") or ""
    base_url = base_url or DEFAULT_BASE_URLS.get(platform, "")

    if platform == "OpenAI":
        return OpenAICompatibleProvider(api_key, base_url, model)
    if platform == "DeepSeek":
        return DeepSeekProvider(api_key, base_url, model)
    if platform == "Claude":
        return ClaudeProvider(api_key, base_url, model)
    if platform == "Ollama":
        return OllamaProvider(api_key or "ollama", base_url, model)
    # 未知平台按 OpenAI 兼容处理
    return OpenAICompatibleProvider(api_key, base_url or "https://api.openai.com/v1", model)
