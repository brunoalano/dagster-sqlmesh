import datetime
import logging

import polars

import pytest

from dagster_sqlmesh.controller.base import PlanOptions, RunOptions, SQLMeshInstance
from sqlmesh.utils.errors import SQLMeshError
from dagster_sqlmesh.testing import SQLMeshTestContext

logger = logging.getLogger(__name__)


def test_basic_sqlmesh_context(sample_sqlmesh_test_context: SQLMeshTestContext):
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 7


def test_sqlmesh_context(sample_sqlmesh_test_context: SQLMeshTestContext):
    logger.debug("SQLMESH MATERIALIZATION 1")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",
        end="2024-01-01",
        execution_time="2024-01-02",
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 5

    logger.debug("SQLMESH MATERIALIZATION 2")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2024-01-01",
        end="2024-07-07",
        execution_time="2024-07-08",
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 7

    test_source_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
    """
    )
    assert test_source_model_count[0][0] == 2

    sample_sqlmesh_test_context.append_to_test_source(
        polars.DataFrame(
            {
                "id": [3, 4, 5],
                "name": ["test", "test", "test"],
            }
        )
    )
    logger.debug("SQLMESH MATERIALIZATION 3")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        end="2024-07-10",
        execution_time="2024-07-10",
        # restate_models=["sqlmesh_example.staging_model_3"],
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 7

    test_source_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
    """
    )
    assert test_source_model_count[0][0] == 5

    logger.debug("SQLMESH MATERIALIZATION 4 - should be no changes")
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        end="2024-07-10",
        execution_time="2024-07-10",
    )

    logger.debug("SQLMESH MATERIALIZATION 5")
    sample_sqlmesh_test_context.append_to_test_source(
        polars.DataFrame(
            {
                "id": [6],
                "name": ["test"],
            }
        )
    )
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        # restate_models=["sqlmesh_example.staging_model_3"],
    )

    print(
        sample_sqlmesh_test_context.query(
            """
    SELECT * FROM sources.test_source
    """
        )
    )

    test_source_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_3
    """
    )
    assert test_source_model_count[0][0] == 6


def test_restating_models(sample_sqlmesh_test_context: SQLMeshTestContext):
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-01-01",
        end="2024-01-01",
        execution_time="2024-01-02",
    )

    count_query = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) FROM sqlmesh_example__dev.staging_model_4
    """
    )
    expected_rows = (datetime.date(2024, 1, 1) - datetime.date(2023, 1, 1)).days
    assert count_query[0][0] == expected_rows

    feb_sum_query = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-02-01' AND time < '2023-02-28'
    """
    )
    march_sum_query = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-03-01' AND time < '2023-03-31'
    """
    )
    intermediate_2_query = sample_sqlmesh_test_context.query(
        """
    SELECT * FROM sqlmesh_example__dev.intermediate_model_2
    """
    )
    assert (
        len(intermediate_2_query) > 0
    ), "Intermediate model should have data prior to restate"

    # Restate the model for the month of March
    sample_sqlmesh_test_context.plan_and_run(
        environment="dev",
        start="2023-03-01",
        end="2023-03-31",
        execution_time="2024-01-02",
        restate_models=["sqlmesh_example.staging_model_4"],
    )

    # Check that the sum of values for February and March are the same
    feb_sum_query_restate = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-02-01' AND time < '2023-02-28'
    """
    )
    march_sum_query_restate = sample_sqlmesh_test_context.query(
        """
    SELECT SUM(value) FROM sqlmesh_example__dev.staging_model_4 WHERE time >= '2023-03-01' AND time < '2023-03-31'
        """
    )
    intermediate_2_query_restate = sample_sqlmesh_test_context.query(
        """
    SELECT * FROM sqlmesh_example__dev.intermediate_model_2
    """
    )

    assert (
        feb_sum_query_restate[0][0] == feb_sum_query[0][0]
    ), "February sum should not change"
    assert (
        march_sum_query_restate[0][0] != march_sum_query[0][0]
    ), "March sum should change"
    assert (
        len(intermediate_2_query_restate) == len(intermediate_2_query)
    ), "Intermediate model rows should be rebuilt during restate"


def test_plan_and_run_skips_explicit_run_when_plan_handles_execution(
    sample_sqlmesh_test_context: SQLMeshTestContext, monkeypatch
):
    controller = sample_sqlmesh_test_context.create_controller()

    run_invoked = False
    original_run = SQLMeshInstance.run

    def tracking_run(self: SQLMeshInstance, **kwargs):
        nonlocal run_invoked
        run_invoked = True
        yield from original_run(self, **kwargs)

    monkeypatch.setattr(SQLMeshInstance, "run", tracking_run)

    plan_options = PlanOptions(
        enable_preview=True,
        run=True,
        execution_time="2024-01-02",
    )
    run_options = RunOptions(execution_time="2024-01-02")

    list(
        controller.plan_and_run(
            "dev",
            start="2023-01-01",
            end="2024-01-01",
            plan_options=plan_options,
            run_options=run_options,
        )
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 5
    assert not run_invoked, "Run stage should not be invoked when plan already executes"


def test_plan_and_run_invokes_run_when_plan_option_disabled(
    sample_sqlmesh_test_context: SQLMeshTestContext, monkeypatch
):
    controller = sample_sqlmesh_test_context.create_controller()

    run_invocations = 0
    original_run = SQLMeshInstance.run

    def tracking_run(self: SQLMeshInstance, **kwargs):
        nonlocal run_invocations
        run_invocations += 1
        yield from original_run(self, **kwargs)

    monkeypatch.setattr(SQLMeshInstance, "run", tracking_run)

    plan_options = PlanOptions(
        enable_preview=True,
        run=False,
        execution_time="2024-01-02",
    )
    run_options = RunOptions(execution_time="2024-01-02")

    list(
        controller.plan_and_run(
            "dev",
            start="2023-01-01",
            end="2024-01-01",
            plan_options=plan_options,
            run_options=run_options,
        )
    )

    staging_model_count = sample_sqlmesh_test_context.query(
        """
    SELECT COUNT(*) as items FROM sqlmesh_example__dev.staging_model_1
    """
    )
    assert staging_model_count[0][0] == 5
    assert run_invocations == 1, "Run stage should execute exactly once when disabled in plan"


def test_plan_and_run_explain_skips_run_stage(
    sample_sqlmesh_test_context: SQLMeshTestContext, monkeypatch
):
    controller = sample_sqlmesh_test_context.create_controller()

    plan_options: PlanOptions = PlanOptions(enable_preview=True)

    # First run populates the environment and allows SQLMesh to set run=True
    list(
        controller.plan_and_run(
            "dev",
            start="2023-01-01",
            end="2023-01-10",
            plan_options=plan_options,
        )
    )

    assert plan_options.get("run") is True

    run_invoked = False
    original_run = SQLMeshInstance.run

    def tracking_run(self: SQLMeshInstance, **kwargs):
        nonlocal run_invoked
        run_invoked = True
        yield from original_run(self, **kwargs)

    monkeypatch.setattr(SQLMeshInstance, "run", tracking_run)

    plan_options["explain"] = True

    with pytest.raises(SQLMeshError):
        list(
            controller.plan_and_run(
                "dev",
                start="2023-01-01",
                end="2023-02-01",
                plan_options=plan_options,
            )
        )

    assert not run_invoked, "Explain mode should not trigger a run stage"
    assert plan_options.get("run") is False
