import inspect
import uuid

from dagster_sqlmesh.console import EventConsole, LogSuccess, UnknownEventCallable


def test_event_console_emits_log_success_event():
    console = EventConsole()
    captured: list[LogSuccess] = []
    handler_id = console.add_handler(lambda event: captured.append(event))

    console.log_success("upgrade complete")

    assert captured, "Expected log_success to emit an event"
    event = captured[-1]
    assert isinstance(event, LogSuccess)
    assert event.message == "upgrade complete"
    assert event.unknown_args == {}

    console.remove_handler(handler_id)


def test_unknown_event_callable_binds_console_self(monkeypatch):
    console = EventConsole()
    captured: dict[str, dict[str, object]] = {}

    def capture_unknown(event_name: str, **kwargs):
        captured["name"] = event_name
        captured["payload"] = kwargs

    monkeypatch.setattr(console, "publish_unknown_event", capture_unknown)

    signature = inspect.Signature(
        parameters=[
            inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter("value", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ]
    )
    callable_handler = UnknownEventCallable(console, "custom_event", signature)

    callable_handler("payload")

    assert captured["name"] == "custom_event"
    assert captured["payload"] == {"value": "payload"}


def test_loading_start_returns_uuid():
    console = EventConsole()
    events = []
    console.add_handler(lambda event: events.append(event))

    loading_id = console.loading_start("starting work")

    assert isinstance(loading_id, uuid.UUID)
    assert events
    assert events[-1].id == loading_id


def test_start_destroy_returns_true():
    console = EventConsole()
    assert console.start_destroy() is True


def test_start_state_export_returns_true(tmp_path):
    console = EventConsole()
    result = console.start_state_export(output_file=tmp_path / "state.json")
    assert result is True
