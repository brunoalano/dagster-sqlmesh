import pytest

import pytest

from dagster_sqlmesh.controller.base import PlanOptions, SQLMeshInstance
from dagster_sqlmesh.testing import SQLMeshTestContext


def test_plan_and_run_rejects_select_models_in_plan_options(
    sample_sqlmesh_test_context: SQLMeshTestContext,
):
    controller = sample_sqlmesh_test_context.create_controller()

    with controller.instance("dev") as instance:
        with pytest.raises(ValueError, match="select_models should not be set"):
            list(
                instance.plan_and_run(
                    plan_options=PlanOptions(select_models=["sqlmesh_example.staging_model_1"])
                )
            )


def test_plan_and_run_rejects_restate_models_in_plan_options(
    sample_sqlmesh_test_context: SQLMeshTestContext,
):
    controller = sample_sqlmesh_test_context.create_controller()

    with controller.instance("dev") as instance:
        with pytest.raises(ValueError, match="restate_models should not be set"):
            list(
                instance.plan_and_run(
                    plan_options=PlanOptions(restate_models=["sqlmesh_example.staging_model_1"])
                )
            )


def test_plan_and_run_applies_restate_selected_to_plan_options(
    sample_sqlmesh_test_context: SQLMeshTestContext, monkeypatch
):
    controller = sample_sqlmesh_test_context.create_controller()

    captured: dict[str, object] = {}
    original_plan_and_run = SQLMeshInstance.plan_and_run

    def tracking_plan_and_run(self: SQLMeshInstance, *args, **kwargs):
        plan_options = kwargs.get("plan_options")
        run_options = kwargs.get("run_options")

        try:
            yield from original_plan_and_run(self, *args, **kwargs)
        finally:
            captured["plan_options"] = dict(plan_options or {})
            captured["run_options"] = dict(run_options or {})
            captured["select_models"] = kwargs.get("select_models")
            captured["restate_models"] = kwargs.get("restate_models")

    monkeypatch.setattr(SQLMeshInstance, "plan_and_run", tracking_plan_and_run)

    select_models = ["sqlmesh_example.staging_model_4"]

    # Ensure the target environment exists prior to scoped run
    sample_sqlmesh_test_context.plan_and_run(environment="dev")

    list(
        controller.plan_and_run(
            "dev",
            start="2023-01-01",
            end="2023-02-01",
            select_models=select_models,
            restate_selected=True,
            plan_options=PlanOptions(enable_preview=True),
            run_options=None,
        )
    )

    assert captured["select_models"] == select_models
    # restate_models argument remains None when not explicitly supplied
    assert captured["restate_models"] in (None, [])

    plan_options = captured["plan_options"]

    assert plan_options["select_models"] == select_models
    assert plan_options["restate_models"] == select_models
    assert plan_options["run"] is True


@pytest.mark.parametrize(
    "additional_options",
    [
        {"ignore_cron": True},
        {"min_intervals": 3},
        {"diff_rendered": True, "skip_linter": True},
    ],
)
def test_plan_and_run_forwards_additional_plan_options(
    sample_sqlmesh_test_context: SQLMeshTestContext,
    monkeypatch,
    additional_options: dict[str, object],
):
    controller = sample_sqlmesh_test_context.create_controller()

    captured: dict[str, dict[str, object]] = {}
    original_plan_and_run = SQLMeshInstance.plan_and_run

    def tracking_plan_and_run(self: SQLMeshInstance, *args, **kwargs):
        plan_options = kwargs.get("plan_options")
        try:
            yield from original_plan_and_run(self, *args, **kwargs)
        finally:
            captured["plan_options"] = dict(plan_options or {})

    monkeypatch.setattr(SQLMeshInstance, "plan_and_run", tracking_plan_and_run)

    # Prime environment to avoid initial plan setup failures
    sample_sqlmesh_test_context.plan_and_run(environment="dev")

    plan_options = PlanOptions(enable_preview=True, **additional_options)

    select_models = ["sqlmesh_example.staging_model_1"]

    list(
        controller.plan_and_run(
            "dev",
            start="2023-01-01",
            end="2023-01-10",
            select_models=select_models,
            plan_options=plan_options,
            skip_run=True,
        )
    )

    forwarded = captured["plan_options"]
    for key, value in additional_options.items():
        assert key in forwarded
        assert forwarded[key] == value
