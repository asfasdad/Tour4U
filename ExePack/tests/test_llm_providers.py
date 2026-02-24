import importlib


llm_providers = importlib.import_module("llm_providers")


def test_openai_compatible_provider_normalizes_base_url_to_v1():
    p = llm_providers.OpenAICompatibleProvider("k", "https://api.openai.com", "gpt-4o")
    assert p._base_url.endswith("/v1")


def test_get_provider_returns_expected_implementations():
    assert isinstance(
        llm_providers.get_provider("OpenAI", "gpt-4o", "k"), llm_providers.OpenAICompatibleProvider
    )
    assert isinstance(
        llm_providers.get_provider("DeepSeek", "deepseek-chat", "k"), llm_providers.DeepSeekProvider
    )
    assert isinstance(
        llm_providers.get_provider("Claude", "claude-3-5-sonnet-20241022", "k"), llm_providers.ClaudeProvider
    )


def test_platform_models_contains_default_codex():
    assert "OpenAI" in llm_providers.PLATFORM_MODELS
    assert "gpt-5.3-codex" in llm_providers.PLATFORM_MODELS["OpenAI"]


def test_get_provider_fallback_for_unknown_platform():
    provider = llm_providers.get_provider("UnknownVendor", "x-model", "k")
    assert isinstance(provider, llm_providers.OpenAICompatibleProvider)


def test_deepseek_provider_uses_default_base_url_when_missing():
    provider = llm_providers.get_provider("DeepSeek", "deepseek-chat", "k", base_url="")
    assert provider._base_url.startswith("https://api.deepseek.com")
