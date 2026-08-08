from types import SimpleNamespace

from nerve.agent.backends.codex.langfuse_canary import _validate_usage


def _observation(usage, cost=None):
    return SimpleNamespace(
        id="generation-1",
        usage_details=usage,
        cost_details=cost or {},
    )


def test_canonical_exclusive_usage_passes():
    errors = _validate_usage([_observation(
        {
            "input": 10,
            "input_cached_tokens": 90,
            "output": 5,
            "output_reasoning_tokens": 15,
            "total": 120,
        },
        {"input_cached_tokens": 0.01, "output_reasoning_tokens": 0.02},
    )])

    assert errors == []


def test_legacy_keys_and_zero_detail_cost_fail():
    legacy = _validate_usage([_observation({
        "input": 100,
        "output": 20,
        "total": 120,
        "cache_read_input_tokens": 90,
        "reasoning_tokens": 15,
    })])
    zero_cost = _validate_usage([_observation({
        "input": 10,
        "input_cached_tokens": 90,
        "output": 5,
        "output_reasoning_tokens": 15,
        "total": 120,
    })])

    assert "legacy usage keys" in legacy[0]
    assert any("input_cached_tokens has zero cost" in error for error in zero_cost)
    assert any("output_reasoning_tokens has zero cost" in error for error in zero_cost)
