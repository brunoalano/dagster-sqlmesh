import logging
import threading
import typing as t
from datetime import UTC, datetime
from types import MappingProxyType

import dagster as dg
import sqlglot
from dagster._core.errors import DagsterInvalidPropertyError
from pydantic import BaseModel, Field
from sqlglot import exp
from sqlmesh import Model
from sqlmesh.core.context import Context as SQLMeshContext
from sqlmesh.core.plan import Plan as SQLMeshPlan
from sqlmesh.core.snapshot import Snapshot, SnapshotInfoLike
from sqlmesh.core.snapshot.definition import Intervals
from sqlmesh.core.table_diff import TableDiff
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import SQLMeshError

from dagster_sqlmesh import console
from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.controller import PlanOptions, RunOptions
from dagster_sqlmesh.controller.base import (
    DEFAULT_CONTEXT_FACTORY,
    ContextCls,
    ContextFactory,
)
from dagster_sqlmesh.controller.dagster import DagsterSQLMeshController
from dagster_sqlmesh.events import ConsoleGenerator

if t.TYPE_CHECKING:
    from dagster_sqlmesh.translator import SQLMeshDagsterTranslator

logger = logging.getLogger(__name__)


def _START_OF_UNIX_TIME():
    dt = datetime.strptime("1970-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
    return dt.astimezone(UTC)


class ModelMaterializationStatus(BaseModel):
    model_fqn: str

    # The last time this model was updated or restated
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    snapshot_id: str

    last_updated_or_restated: datetime = Field(default_factory=_START_OF_UNIX_TIME)
    last_promoted: datetime = Field(default_factory=_START_OF_UNIX_TIME)
    last_backfill: datetime = Field(default_factory=_START_OF_UNIX_TIME)

    def update_or_restate_now(self):
        """Shortcut function to set last_updated_or_restated time to now"""
        self.last_updated_or_restated = datetime.now(UTC)

    def promote_now(self):
        """Shortcut function to set last_promoted time to now"""
        self.last_promoted = datetime.now(UTC)

    def backfill_now(self):
        """Shortcut function to set last_backfill time to now"""
        self.last_backfill = datetime.now(UTC)

    def as_dagster_metadata(
        self, previous: "ModelMaterializationStatus | None"
    ) -> dict[str, dg.MetadataValue]:
        if previous:
            # if the previous materialization status exists then we compare all
            # of the dates and take the _largest_ for all dates except
            # `created_at`
            last_updated_or_restated = dg.MetadataValue.timestamp(
                max(previous.last_updated_or_restated, self.last_updated_or_restated)
            )
            last_promoted = dg.MetadataValue.timestamp(
                max(previous.last_promoted, self.last_promoted)
            )
            last_backfill = dg.MetadataValue.timestamp(
                max(previous.last_backfill, self.last_backfill)
            )
            created_at = dg.MetadataValue.timestamp(previous.created_at)
        else:
            # If there is no previous materialization status all dates can use
            # the created_at timestamp
            created_at = dg.MetadataValue.timestamp(self.created_at)
            last_updated_or_restated = dg.MetadataValue.timestamp(self.created_at)
            last_promoted = dg.MetadataValue.timestamp(self.created_at)
            last_backfill = dg.MetadataValue.timestamp(self.created_at)

        return {
            "snapshot_id": dg.MetadataValue.text(self.snapshot_id),
            "model_fqn": dg.MetadataValue.text(self.model_fqn),
            "created_at": created_at,
            "last_updated_or_restated": last_updated_or_restated,
            "last_promoted": last_promoted,
            "last_backfill": last_backfill,
        }

    @classmethod
    def from_dagster_metadata(
        cls, metadata: dict[str, t.Any]
    ) -> "ModelMaterializationStatus":
        # convert metadata values
        converted: dict[str, dg.MetadataValue] = {}
        for key, value in metadata.items():
            assert isinstance(
                value, dg.MetadataValue
            ), f"Expected MetadataValue for {key}, got {type(value)}"
            converted[key] = value

        return cls.model_validate(
            dict(
                model_fqn=converted["model_fqn"].value,
                snapshot_id=converted["snapshot_id"].value,
                created_at=converted["created_at"].value,
                last_updated_or_restated=converted["last_updated_or_restated"].value,
                last_promoted=converted["last_promoted"].value,
                last_backfill=converted["last_backfill"].value,
            )
        )

    def as_glot_table(self) -> exp.Table:
        return sqlglot.to_table(self.model_fqn)

    def is_match(self, input: str, ignore_catalog: bool = False) -> bool:
        """Tests if the passed in string matches this model's table

        Args:
            input (str): The input string to match against the model's table.
            ignore_catalog (bool): Whether to use to only match table db and
            name (default: False)

        Returns:
            bool - True if the input string matches the model's table, False
            otherwise.
        """
        table = self.as_glot_table()

        input_as_table = sqlglot.to_table(input)

        if input_as_table.name != table.name:
            return False
        if input_as_table.db != table.db:
            return False

        if not ignore_catalog:
            if input_as_table.catalog != table.catalog:
                return False
        return True


class MaterializationTracker:
    """Tracks sqlmesh materializations and notifies dagster in the correct
    order. This is necessary because sqlmesh may skip some materializations that
    have no changes and those will be reported as completed out of order."""

    def __init__(self, sorted_dag: list[str], logger: logging.Logger) -> None:
        self.logger = logger
        self._batches: dict[Snapshot, int] = {}
        self._count: dict[Snapshot, int] = {}
        self._model_metadata: dict[str, ModelMaterializationStatus] = {}
        self._non_model_names: set[str] = set()
        self._sorted_dag = sorted_dag
        self._current_index = 0
        self.finished_promotion = False

    def initialize_from_plan(self, plan: SQLMeshPlan):
        # Initialize all of the model materialization statuses
        # Existing snapshots
        snapshots_by_name = {
            snapshot.name: snapshot for snapshot in plan.snapshots.values()
        }

        created_at = datetime.now(UTC)

        # Include new snapshots
        for snapshot in plan.context_diff.new_snapshots.values():
            snapshots_by_name[snapshot.name] = snapshot

        for model_fqn in self._sorted_dag:
            snapshot = snapshots_by_name.get(model_fqn)

            if not snapshot:
                self._non_model_names.add(model_fqn)
                continue

            if snapshot.is_external:
                self._non_model_names.add(model_fqn)
                continue

            self._model_metadata[snapshot.name] = ModelMaterializationStatus(
                model_fqn=snapshot.model.fqn,
                snapshot_id=snapshot.identifier,
                created_at=created_at,
            )

        # Update all of the model status that are to be updated or restated in this plan
        # This condition was taken from a condition found in sqlmesh's `Context`
        # object. It's used to determine if there are any changes in the plan
        if (
            not plan.context_diff.has_changes
            and not plan.requires_backfill
            and not plan.has_unmodified_unpromoted
        ):
            self.logger.info("No changes detected, adding all models to the queue")
        else:
            context_diff = plan.context_diff
            for snapshot in context_diff.new_snapshots.values():
                self._model_metadata[snapshot.name].update_or_restate_now()
            for snapshots in context_diff.modified_snapshots.values():
                self._model_metadata[snapshots[0].name].update_or_restate_now()
            for snapshot_id in plan.restatements.keys():
                self._model_metadata[
                    plan.snapshots[snapshot_id].name
                ].update_or_restate_now()

    def update_promotion(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        if promoted:
            self._model_metadata[snapshot.name].promote_now()

    def update_run(self, snapshot: SnapshotInfoLike) -> None:
        self._model_metadata[snapshot.name].backfill_now()

    def stop_promotion(self) -> None:
        self.finished_promotion = True

    def plan(self, batches: dict[Snapshot, Intervals]) -> None:
        self._batches = {}
        self._count = {}

        for snapshot, intervals in batches.items():
            if hasattr(intervals, "__len__"):
                expected = len(intervals)  # type: ignore[arg-type]
            else:
                raw_intervals = getattr(intervals, "intervals", [])
                expected = len(list(raw_intervals))
            self._batches[snapshot] = expected
            self._count[snapshot] = 0
            self._model_metadata[snapshot.name].backfill_now()

    def update_plan(self, snapshot: Snapshot, _batch_idx: int) -> tuple[int, int]:
        self._count[snapshot] += 1
        current_count = self._count[snapshot]
        expected_count = self._batches[snapshot]
        return (current_count, expected_count)

    def notify_queue_next(self) -> tuple[str, ModelMaterializationStatus] | None:
        """Notifies about the next materialization in the queue. At the end of a
        sqlmesh run the `all_up_to_date` flag should be set to True.

        Returns:
            A tuple containing the name of the materialization and its status,
            or None if there are no more model statuses left in the queue
        """
        if self._current_index >= len(self._sorted_dag):
            return None
        self.logger.debug(
            f"MaterializationTracker index {self._current_index}",
            extra=dict(
                current_index=self._current_index,
            ),
        )

        while True:
            model_name_for_notification = self._sorted_dag[self._current_index]

            if model_name_for_notification in self._non_model_names:
                self._current_index += 1
                self.logger.debug(
                    f"skipping non-model snapshot {model_name_for_notification}"
                )
                continue

            if model_name_for_notification in self._model_metadata:
                self._current_index += 1
                return (
                    model_name_for_notification,
                    self._model_metadata[model_name_for_notification],
                )
            return None


class SQLMeshEventLogContext:
    def __init__(
        self,
        handler: "DagsterSQLMeshEventHandler",
        event: console.ConsoleEvent,
    ):
        self._handler = handler
        self._event = event

    def ensure_standard_obj(self, obj: dict[str, t.Any] | None) -> dict[str, t.Any]:
        obj = obj or {}
        obj["_event_type"] = self.event_name
        return obj

    def info(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("info", message, obj)

    def debug(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("debug", message, obj)

    def warning(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("warning", message, obj)

    def error(self, message: str, obj: dict[str, t.Any] | None = None) -> None:
        self.log("error", message, obj)

    def log(self, level: str | int, message: str, obj: dict[str, t.Any] | None) -> None:
        self._handler.log(level, message, self.ensure_standard_obj(obj))

    @property
    def event_name(self):
        return self._event.__class__.__name__


class GenericSQLMeshError(Exception):
    pass


class FailedModelError(Exception):
    def __init__(self, model_name: str, message: str | None) -> None:
        super().__init__(message)
        self.model_name = model_name
        self.message = message


class PlanOrRunFailedError(Exception):
    def __init__(self, stage: str, message: str, errors: list[Exception]) -> None:
        super().__init__(message)
        self.stage = stage
        self.errors = errors


class DagsterSQLMeshEventHandler:
    def __init__(
        self,
        context: dg.AssetExecutionContext,
        models_map: dict[str, Model],
        dag: DAG[t.Any],
        prefix: str,
        translator: "SQLMeshDagsterTranslator",
        is_testing: bool = False,
        materializations_enabled: bool = True,
    ) -> None:
        """Dagster event handler for SQLMesh models.

        The handler is responsible for reporting events from sqlmesh to dagster.

        Args:
            context: The Dagster asset execution context.
            models_map: A mapping of model names to their SQLMesh model instances.
            dag: The directed acyclic graph representing the SQLMesh models.
            prefix: A prefix to use for all asset keys generated by this handler.
            translator: The SQLMesh Dagster translator instance.
            is_testing: Whether the handler is being used in a testing context.
            materializations_enabled: Whether the handler is to generate
                materializations, this should be disabled if you with to run a
                sqlmesh plan or run in an environment different from the normal
                target environment.
        """
        self._models_map = models_map
        self._prefix = prefix
        self._context = context
        self._logger = context.log
        self._translator = translator
        self._tracker = MaterializationTracker(
            sorted_dag=dag.sorted[:], logger=self._logger
        )
        self._stage = "plan"
        self._errors: list[Exception] = []
        self._is_testing = is_testing
        self._materializations_enabled = materializations_enabled

    def process_events(self, event: console.ConsoleEvent) -> None:
        self.report_event(event)

    def notify_success(
        self, sqlmesh_context: SQLMeshContext
    ) -> t.Iterator[dg.MaterializeResult]:
        notify = self._tracker.notify_queue_next()

        while notify is not None:
            completed_name, materialization_status = notify

            # If the model is not in the context, we can skip any notification
            # This will happen for external models
            if not sqlmesh_context.get_model(completed_name):
                notify = self._tracker.notify_queue_next()
                continue

            model = self._models_map.get(completed_name)

            # We allow selecting models. That value is mapped to models_map.
            # If the model is not in models_map, we can skip any notification
            if model:
                # Passing model.fqn to get internal unique asset key
                output_key = self._translator.get_asset_key_str(model.fqn)
                if self._is_testing:
                    asset_key = dg.AssetKey(["testing", output_key])
                    self._logger.warning(
                        f"Generated fake asset key for testing: {asset_key.to_user_string()}"
                    )
                else:
                    asset_key = self._context.asset_key_for_output(output_key)
                if self._materializations_enabled:
                    yield self.create_materialize_result(
                        self._context, asset_key, materialization_status
                    )
                else:
                    self._logger.debug(
                        f"Materializations disabled. Would have materialized for {asset_key.to_user_string()}"
                    )
            notify = self._tracker.notify_queue_next()
        else:
            self._logger.debug("No more materializations to process")

    def create_materialize_result(
        self,
        context: dg.AssetExecutionContext,
        asset_key: dg.AssetKey,
        current_materialization_status: ModelMaterializationStatus,
    ) -> dg.MaterializeResult:
        last_materialization = context.instance.get_latest_materialization_event(
            asset_key
        )

        if not last_materialization:
            self._logger.debug(
                f"No materialization found for {asset_key.to_user_string()}, all dates will be set to now."
            )
            last_materialization_status = None
        else:
            assert (
                last_materialization.asset_materialization is not None
            ), "Expected asset materialization to be present."
            try:
                last_materialization_status = (
                    ModelMaterializationStatus.from_dagster_metadata(
                        dict(last_materialization.asset_materialization.metadata)
                    )
                )
            except Exception as e:
                self._logger.warning(
                    f"Failed to validate last materialization for {asset_key.to_user_string()}: {e}. Ignoring and using the current status"
                )
                last_materialization_status = None

        return dg.MaterializeResult(
            asset_key=asset_key,
            metadata=current_materialization_status.as_dagster_metadata(
                last_materialization_status
            ),
        )

    def report_event(self, event: console.ConsoleEvent) -> None:
        log_context = self.log_context(event)

        match event:
            case console.PlanBuilt(plan=plan):
                log_context.info(
                    "Plan built",
                    {
                        "snapshots": [s.name for s in plan.environment.snapshots],
                        "models_to_backfill": plan.models_to_backfill,
                        "empty_backfill": plan.empty_backfill,
                        "requires_backfill": plan.requires_backfill,
                    },
                )
                self._tracker.initialize_from_plan(plan)
            case console.StartPlanEvaluation(plan=plan):
                log_context.info(
                    "Starting Plan Evaluation",
                    {
                        "plan": plan,
                    },
                )
            case console.StopPlanEvaluation:
                log_context.info("Plan evaluation completed")
            case console.StartEvaluationProgress(
                batched_intervals=batches,
                environment_naming_info=environment_naming_info,
                default_catalog=default_catalog,
                audit_only=audit_only,
            ):
                self.update_stage("run")
                log_context.info(
                    "Starting Run",
                    {
                        "default_catalog": default_catalog,
                        "environment_naming_info": environment_naming_info,
                        "audit_only": audit_only,
                        "backfill_queue": {
                            snapshot.model.name: len(intervals)
                            for snapshot, intervals in batches.items()
                        },
                    },
                )
                self._tracker.plan(batches)
            case console.UpdateSnapshotEvaluationProgress(
                snapshot=snapshot,
                interval=interval,
                batch_idx=batch_idx,
                duration_ms=duration_ms,
                num_audits_passed=num_audits_passed,
                num_audits_failed=num_audits_failed,
                audit_only=audit_only,
                execution_stats=execution_stats,
                auto_restatement_triggers=auto_restatement_triggers,
            ):
                done, expected = self._tracker.update_plan(snapshot, batch_idx)

                if done == expected:
                    log_context.info(
                        "Snapshot progress complete",
                        {
                            "asset_key": self._translator.get_asset_key_str(snapshot.model.name),
                        },
                    )
                    self._tracker.update_run(snapshot)
                else:
                    log_context.info(
                        "Snapshot progress update",
                        {
                            "asset_key": self._translator.get_asset_key_str(snapshot.model.name),
                            "progress": f"{done}/{expected}",
                            "duration_ms": duration_ms,
                            "interval": {
                                "start": interval.start,
                                "end": interval.end,
                            },
                            "audit_only": audit_only,
                            "audits_passed": num_audits_passed,
                            "audits_failed": num_audits_failed,
                            "execution_stats": execution_stats,
                            "auto_restatement_triggers": auto_restatement_triggers,
                        },
                    )
            case console.LogSuccess(success=success):
                self.update_stage("done")
                if success:
                    log_context.info("sqlmesh ran successfully")
                else:
                    log_context.error("sqlmesh failed. check collected errors")
            case console.LogError(message=message):
                log_context.error(
                    f"sqlmesh reported an error: {message}",
                )
                self._errors.append(GenericSQLMeshError(message))
            case console.LogFailedModels(errors=errors):
                if len(errors) != 0:
                    failed_models = "\n".join(
                        [f"{error.node!s}\n{error.__cause__!s}" for error in errors]
                    )
                    log_context.error(f"sqlmesh failed models: {failed_models}")
                    for error in errors:
                        self._errors.append(
                            FailedModelError(error.node, str(error.__cause__))
                        )
            case console.LogAdditiveChange(
                snapshot_name=snapshot_name,
                alter_operations=alter_operations,
                dialect=dialect,
                error=error,
            ):
                log_context.info(
                    "Additive change detected",
                    {
                        "snapshot": snapshot_name,
                        "dialect": dialect,
                        "operations": [str(op) for op in alter_operations],
                        "error": error,
                    },
                )
            case console.LogModelsUpdatedDuringRestatement(
                snapshots=snapshots,
                environment_naming_info=environment_naming_info,
                default_catalog=default_catalog,
            ):
                log_context.info(
                    "Models updated during restatement",
                    {
                        "snapshots": [
                            {
                                "from": previous.name,
                                "to": current.name,
                            }
                            for current, previous in snapshots
                        ],
                        "environment_naming_info": environment_naming_info,
                        "default_catalog": default_catalog,
                    },
                )
            case console.ShowEnvironmentDifferenceSummary(
                context_diff=context_diff, no_diff=no_diff
            ):
                log_context.info(
                    "Environment difference summary",
                    {
                        "no_diff": no_diff,
                        "added": list(context_diff.added),
                        "removed": list(context_diff.removed_snapshots.keys()),
                        "modified": list(context_diff.modified_snapshots.keys()),
                    },
                )
            case console.ShowIntervals(snapshot_intervals=snapshot_intervals):
                log_context.info(
                    "Snapshot intervals",
                    {
                        "snapshots": {
                            snapshot.name: [
                                {
                                    "start": interval.start,
                                    "end": interval.end,
                                }
                                for interval in intervals
                            ]
                            for snapshot, intervals in snapshot_intervals.items()
                        }
                    },
                )
            case console.ShowLinterViolations(
                violations=violations, model=model, is_error=is_error
            ):
                log_context.warning(
                    "Linter violations reported",
                    {
                        "model": model.fqn if model else None,
                        "count": len(violations),
                        "severity": "error" if is_error else "warning",
                    },
                )
            case console.UpdatePromotionProgress(snapshot=snapshot, promoted=promoted):
                log_context.info(
                    "Promotion progress update",
                    {
                        "snapshot": snapshot.name,
                        "promoted": promoted,
                    },
                )
                self._tracker.update_promotion(snapshot, promoted)
            case console.StopPromotionProgress(success=success):
                self._tracker.stop_promotion()
                if success:
                    log_context.info("Promotion completed successfully")
                else:
                    log_context.error("Promotion failed")
            case console.StartDestroy(
                schemas_to_delete=schemas,
                views_to_delete=views,
                tables_to_delete=tables,
            ):
                log_context.info(
                    "Destroy operation started",
                    {
                        "schemas": sorted(schemas or []),
                        "views": sorted(views or []),
                        "tables": sorted(tables or []),
                    },
                )
            case console.StopDestroy(success=success):
                if success:
                    log_context.info("Destroy operation completed")
                else:
                    log_context.error("Destroy operation failed")
            case console.ShowTableDiff(table_diffs=table_diffs) as diff_event:
                log_context.info(
                    "Table diff results",
                    {
                        "models": [diff.model_name for diff in table_diffs],
                        "options": {
                            "show_sample": diff_event.show_sample,
                            "skip_grain_check": diff_event.skip_grain_check,
                            "temp_schema": diff_event.temp_schema,
                        },
                    },
                )
            case console.ShowTableDiffDetails(models_to_diff=models_to_diff):
                log_context.info(
                    "Table diff model details",
                    {
                        "models": models_to_diff,
                    },
                )
            case console.StartSignalProgress(
                snapshot=snapshot,
                default_catalog=default_catalog,
                environment_naming_info=environment_naming_info,
            ):
                log_context.info(
                    "Signal evaluation started",
                    {
                        "snapshot": snapshot.name,
                        "default_catalog": default_catalog,
                        "environment_naming_info": environment_naming_info,
                    },
                )
            case console.UpdateSignalProgress(
                snapshot=snapshot,
                signal_name=signal_name,
                signal_idx=signal_idx,
                total_signals=total_signals,
                ready_intervals=ready_intervals,
                check_intervals=check_intervals,
                duration=duration,
            ):
                log_context.info(
                    "Signal progress update",
                    {
                        "snapshot": snapshot.name,
                        "signal_name": signal_name,
                        "position": f"{signal_idx}/{total_signals}",
                        "ready_intervals": [
                            {
                                "start": interval.start,
                                "end": interval.end,
                            }
                            for interval in ready_intervals
                        ],
                        "check_intervals": [
                            {
                                "start": interval.start,
                                "end": interval.end,
                            }
                            for interval in check_intervals
                        ],
                        "duration": duration,
                    },
                )
            case console.StopSignalProgress():
                log_context.info("Signal evaluation finished")
            case console.StartStateExport(
                output_file=output_file,
                gateway=gateway,
                state_connection_config=state_connection_config,
                environment_names=environment_names,
                local_only=local_only,
                confirm=confirm,
            ):
                log_context.info(
                    "State export started",
                    {
                        "output_file": str(output_file),
                        "gateway": gateway,
                        "environment_names": environment_names,
                        "local_only": local_only,
                        "confirm": confirm,
                        "state_connection_config": state_connection_config,
                    },
                )
            case console.UpdateStateExportProgress(
                version_count=version_count,
                versions_complete=versions_complete,
                snapshot_count=snapshot_count,
                snapshots_complete=snapshots_complete,
                environment_count=environment_count,
                environments_complete=environments_complete,
            ):
                log_context.info(
                    "State export progress",
                    {
                        "version_count": version_count,
                        "versions_complete": versions_complete,
                        "snapshot_count": snapshot_count,
                        "snapshots_complete": snapshots_complete,
                        "environment_count": environment_count,
                        "environments_complete": environments_complete,
                    },
                )
            case console.StopStateExport(success=success, output_file=output_file):
                message = "State export completed" if success else "State export failed"
                log_context.info(message, {"output_file": str(output_file), "success": success})
            case console.StartStateImport(
                input_file=input_file,
                gateway=gateway,
                state_connection_config=state_connection_config,
                clear=clear,
                confirm=confirm,
            ):
                log_context.info(
                    "State import started",
                    {
                        "input_file": str(input_file),
                        "gateway": gateway,
                        "clear": clear,
                        "confirm": confirm,
                        "state_connection_config": state_connection_config,
                    },
                )
            case console.UpdateStateImportProgress(
                timestamp=timestamp,
                state_file_version=state_file_version,
                versions=versions,
                snapshot_count=snapshot_count,
                snapshots_complete=snapshots_complete,
                environment_count=environment_count,
                environments_complete=environments_complete,
            ):
                log_context.info(
                    "State import progress",
                    {
                        "timestamp": timestamp,
                        "state_file_version": state_file_version,
                        "versions": versions,
                        "snapshot_count": snapshot_count,
                        "snapshots_complete": snapshots_complete,
                        "environment_count": environment_count,
                        "environments_complete": environments_complete,
                    },
                )
            case console.StopStateImport(success=success, input_file=input_file):
                message = "State import completed" if success else "State import failed"
                log_context.info(message, {"input_file": str(input_file), "success": success})
            case console.StartTableDiffModelProgress(model=model_name):
                log_context.info(
                    "Table diff model progress started",
                    {"model": model_name},
                )
            case console.StartTableDiffProgress(models_to_diff=models_to_diff):
                log_context.info(
                    "Table diff progress started",
                    {"models_to_diff": models_to_diff},
                )
            case console.UpdateTableDiffProgress(model=model_name):
                log_context.info(
                    "Table diff progress update",
                    {"model": model_name},
                )
            case console.StopTableDiffProgress(success=success):
                if success:
                    log_context.info("Table diff completed")
                else:
                    log_context.error("Table diff failed")
            case _:
                log_context.debug("Received event")

    def log_context(self, event: console.ConsoleEvent) -> SQLMeshEventLogContext:
        return SQLMeshEventLogContext(self, event)

    def log(
        self,
        level: str | int,
        message: str,
        obj: dict[str, t.Any] | None = None,
    ) -> None:
        if level == "error":
            self._logger.error(message)
            return

        obj = obj or {}
        final_obj = obj.copy()
        final_obj["message"] = message
        final_obj["_sqlmesh_stage"] = self._stage
        self._logger.log(level, final_obj)

    def update_stage(self, stage: str):
        self._stage = stage

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def errors(self) -> list[Exception]:
        return self._errors[:]


class TableDiffEventHandler:
    """Lightweight handler for SQLMesh table diff console events."""

    def __init__(self, context: dg.AssetExecutionContext):
        self._context = context

    def process_events(self, event: console.ConsoleEvent) -> None:
        log = self._context.log

        match event:
            case console.ShowTableDiff(table_diffs=table_diffs) as diff_event:
                log.info(
                    "SQLMesh table diff results",
                    {
                        "models": [getattr(diff, "source_schema", str(diff)) for diff in table_diffs],
                        "options": {
                            "show_sample": diff_event.show_sample,
                            "skip_grain_check": diff_event.skip_grain_check,
                            "temp_schema": diff_event.temp_schema,
                        },
                    },
                )
            case console.ShowTableDiffDetails(models_to_diff=models_to_diff):
                log.info("SQLMesh table diff details", {"models": models_to_diff})
            case console.ShowTableDiffSummary(table_diff=table_diff):
                log.info(
                    "SQLMesh table diff summary",
                    {
                        "source_schema": getattr(table_diff, "source_schema", None),
                        "target_schema": getattr(table_diff, "target_schema", None),
                        "row_diff": str(getattr(table_diff, "row_diff", "")),
                        "schema_diff": str(getattr(table_diff, "schema_diff", "")),
                    },
                )
            case console.StartTableDiffModelProgress(model=model):
                log.info("Starting table diff for model", {"model": model})
            case console.StartTableDiffProgress(models_to_diff=models_to_diff):
                log.info("Table diff progress started", {"models_to_diff": models_to_diff})
            case console.UpdateTableDiffProgress(model=model):
                log.info("Table diff progress update", {"model": model})
            case console.StopTableDiffProgress(success=success):
                if success:
                    log.info("Table diff completed successfully")
                else:
                    log.error("Table diff failed")
            case console.LogStatusUpdate(message=message):
                log.info(message)
            case console.LogWarning(short_message=short_message, long_message=long_message):
                detail = f"{short_message}: {long_message}" if long_message else short_message
                log.warning(detail)
            case console.LogError(message=message):
                log.error(message)
            case console.LoadingStart(id=load_id, message=message):
                log.debug(
                    "SQLMesh loading start",
                    {"message": message, "id": str(load_id)},
                )
            case console.LoadingStop(id=load_id):
                log.debug("SQLMesh loading stop", {"id": str(load_id)})
            case _:
                log.debug(
                    "Unhandled SQLMesh table diff event",
                    {"event": event.__class__.__name__},
                )


class SQLMeshResource(dg.ConfigurableResource):
    config: SQLMeshContextConfig
    is_testing: bool = False

    def run(
        self,
        context: dg.AssetExecutionContext,
        *,
        context_factory: ContextFactory[ContextCls] = DEFAULT_CONTEXT_FACTORY,
        environment: str = "dev",
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        restate_models: list[str] | None = None,
        select_models: list[str] | None = None,
        restate_selected: bool = False,
        skip_run: bool = False,
        plan_options: PlanOptions | None = None,
        run_options: RunOptions | None = None,
        materializations_enabled: bool = True,
    ) -> t.Iterable[dg.MaterializeResult]:
        """Execute SQLMesh based on the configuration given"""
        plan_options = plan_options or {}
        run_options = run_options or {}

        logger = context.log

        controller = self.get_controller(
            context_factory=context_factory, log_override=logger
        )

        with controller.instance(environment) as mesh:
            dag = mesh.models_dag()

            models = mesh.models()
            models_map = models.copy()
            all_available_models = set(
                [model.fqn for model, _ in mesh.non_external_models_dag()]
            )
            selected_models_set, models_map, select_models = (
                self._get_selected_models_from_context(context=context, models=models)
            )

            if all_available_models == selected_models_set or select_models is None:
                logger.info("all models selected")

                # Setting this to none to allow sqlmesh to select all models and
                # also remove any models
                select_models = None
            else:
                logger.info(f"selected models: {select_models}")

            event_handler = self.create_event_handler(
                context=context,
                models_map=models_map,
                dag=dag,
                prefix="sqlmesh: ",
                is_testing=self.is_testing,
                materializations_enabled=materializations_enabled,
            )

            def raise_for_sqlmesh_errors(
                event_handler: DagsterSQLMeshEventHandler,
                additional_errors: list[Exception] | None = None,
            ) -> None:
                additional_errors = additional_errors or []
                errors = event_handler.errors
                if len(errors) + len(additional_errors) == 0:
                    return
                for error in errors:
                    logger.error(
                        f"sqlmesh encountered the following error during sqlmesh {event_handler.stage}: {error}"
                    )
                raise PlanOrRunFailedError(
                    event_handler.stage,
                    f"sqlmesh failed during {event_handler.stage} with {len(event_handler.errors) + 1} errors",
                    [*errors, *additional_errors],
                )

            try:
                for event in mesh.plan_and_run(
                    start=start,
                    end=end,
                    select_models=select_models,
                    restate_models=restate_models,
                    restate_selected=restate_selected,
                    skip_run=skip_run,
                    plan_options=plan_options,
                    run_options=run_options,
                ):
                    logger.debug(f"sqlmesh event: {event}")
                    event_handler.process_events(event)
            except SQLMeshError as e:
                logger.error(f"sqlmesh error: {e}")
                raise_for_sqlmesh_errors(event_handler, [GenericSQLMeshError(str(e))])
            logger.info(f"sqlmesh run completed for {len(models_map)} models")
            # Some errors do not raise exceptions immediately, so we need to check
            # the event handler for any errors that may have been collected.
            raise_for_sqlmesh_errors(event_handler)

            yield from event_handler.notify_success(mesh.context)

            logger.debug("sqlmesh selected all models notified of completion")

    def table_diff(
        self,
        context: dg.AssetExecutionContext,
        *,
        source: str,
        target: str,
        context_factory: ContextFactory[ContextCls] = DEFAULT_CONTEXT_FACTORY,
        environment: str = "dev",
        **table_diff_kwargs: t.Any,
    ) -> list[TableDiff]:
        """Execute SQLMesh table diff and emit console telemetry through Dagster logs."""

        table_diff_kwargs.setdefault("show", True)

        controller = self.get_controller(
            context_factory=context_factory, log_override=context.log
        )

        with controller.instance(environment, "table_diff") as mesh:
            diffs: list[TableDiff] = []
            errors: list[Exception] = []
            event_handler = TableDiffEventHandler(context)
            generator = ConsoleGenerator(context.log)

            def run_table_diff() -> None:
                try:
                    result = mesh.context.table_diff(source, target, **table_diff_kwargs)
                    diffs.extend(result)
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)
                    mesh.console.exception(exc)

            with mesh.console_context(generator):
                thread = threading.Thread(
                    target=run_table_diff,
                    name="sqlmesh-table-diff",
                )
                thread.start()

                for event in generator.events(thread):
                    match event:
                        case console.ConsoleException(exception=exception):
                            errors.append(exception)
                        case _:
                            event_handler.process_events(event)

                thread.join()

            if errors:
                raise PlanOrRunFailedError(
                    "table_diff",
                    f"sqlmesh failed during table diff with {len(errors)} errors",
                    errors,
                )

            return diffs

    def create_event_handler(
        self,
        *,
        context: dg.AssetExecutionContext,
        dag: DAG[str],
        models_map: dict[str, Model],
        prefix: str,
        is_testing: bool,
        materializations_enabled: bool,
    ) -> DagsterSQLMeshEventHandler:
        translator = self.config.get_translator()
        return DagsterSQLMeshEventHandler(
            context=context,
            dag=dag,
            models_map=models_map,
            prefix=prefix,
            translator=translator,
            is_testing=is_testing,
            materializations_enabled=materializations_enabled,
        )

    def _get_selected_models_from_context(
        self, context: dg.AssetExecutionContext, models: MappingProxyType[str, Model]
    ) -> tuple[set[str], dict[str, Model], list[str] | None]:
        models_map = models.copy()
        try:
            selected_output_names = set(context.op_execution_context.selected_output_names)
        except (DagsterInvalidPropertyError, AttributeError) as e:
            # Special case for direct execution context when testing. This is related to:
            # https://github.com/dagster-io/dagster/issues/23633
            if "DirectOpExecutionContext" in str(e):
                context.log.warning("Caught an error that is likely a direct execution")
                return (set(models_map.keys()), models_map, None)
            else:
                raise e

        translator = self.config.get_translator()
        select_models: list[str] = []
        models_map = {}
        for key, model in models.items():
            if translator.get_asset_key_str(model.fqn) in selected_output_names:
                models_map[key] = model
                select_models.append(model.name)
        return (
            set(models_map.keys()),
            models_map,
            select_models,
        )

    def get_controller(
        self,
        context_factory: ContextFactory[ContextCls],
        log_override: logging.Logger | None = None,
    ) -> DagsterSQLMeshController[ContextCls]:
        return DagsterSQLMeshController.setup_with_config(
            config=self.config,
            context_factory=context_factory,
            log_override=log_override,
        )
