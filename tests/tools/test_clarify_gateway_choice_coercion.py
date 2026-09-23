"""RED tests for the brief's guardrail: clarify_gateway.register must never hold a
choices-bearing entry whose choices are not strings — a non-string list there renders a
buttonless card on Telegram (rows built via str(c)) while the tool result still echoes
the choices, silently degrading the question. The fix: coerce {label,description}-shaped
dicts to 'label — description' strings at the boundary and reject non-string items that
carry no label, instead of rendering a choices-bearing button-less prompt.
"""

from __future__ import annotations


def _clear():
    from tools import clarify_gateway as cm
    with cm._lock:
        cm._entries.clear()
        cm._session_index.clear()
        cm._notify_cbs.clear()


class TestRegisterCoercesDictChoices:
    def setup_method(self):
        _clear()

    def test_label_description_dicts_coerce_to_label_em_dash_description(self):
        from tools import clarify_gateway as cm
        entry = cm.register("c1", "s1", "Pick?", [{"label": "A", "description": "x"},
                                                   {"label": "B", "description": "y"}])
        assert entry.choices == ["A — x", "B — y"]

    def test_label_only_dict_coerces_to_label(self):
        from tools import clarify_gateway as cm
        entry = cm.register("c2", "s2", "Pick?", [{"label": "A"}, "B"])
        assert entry.choices == ["A", "B"]

    def test_description_only_dict_coerces_to_description(self):
        from tools import clarify_gateway as cm
        entry = cm.register("c3", "s3", "Pick?", [{"description": "just a desc"}])
        assert entry.choices == ["just a desc"]

    def test_labelless_dict_raises_naming_the_item(self):
        from tools import clarify_gateway as cm
        import pytest
        with pytest.raises(ValueError) as ei:
            cm.register("c4", "s4", "Pick?", ["A", {"value": "b", "name": "opt_b"}])
        msg = str(ei.value)
        assert "choices[1]" in msg
        assert "not a string" in msg
        # nothing registered — a half-armed button-less prompt is the bug
        assert cm._entries.get("c4") is None

    def test_non_string_scalar_item_raises(self):
        from tools import clarify_gateway as cm
        import pytest
        with pytest.raises(ValueError):
            cm.register("c5", "s5", "Pick?", [{"nope": 1}])

    def test_plain_string_choices_pass_through_unchanged(self):
        from tools import clarify_gateway as cm
        entry = cm.register("c6", "s6", "Pick?", ["A", "B", "C"])
        assert entry.choices == ["A", "B", "C"]
        assert entry.awaiting_text is False

    def test_none_choices_stays_open_text(self):
        from tools import clarify_gateway as cm
        entry = cm.register("c7", "s7", "Free?", None)
        assert entry.choices is None
        assert entry.awaiting_text is True
