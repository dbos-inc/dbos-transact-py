import json
from dataclasses import asdict

from dbos._conductor import protocol as p
from dbos._sys_db import StepInfo, WorkflowStatus


def populated_workflow_status() -> WorkflowStatus:
    # Every field gets a distinct value so a swapped mapping fails.
    info = WorkflowStatus()
    info.workflow_id = "wf-id"
    info.status = "ENQUEUED"
    info.name = "wf-name"
    info.class_name = "wf-class"
    info.config_name = "wf-config"
    info.authenticated_user = "wf-user"
    info.assumed_role = "wf-role"
    info.authenticated_roles = ["wf-role", "wf-other-role"]
    info.input = {"args": (1,), "kwargs": {"k": "v"}}
    info.output = "wf-output"
    info.error = ValueError("wf-error")
    info.created_at = 1001
    info.updated_at = 1002
    info.queue_name = "wf-queue"
    info.executor_id = "wf-executor"
    info.app_version = "wf-version"
    info.workflow_timeout_ms = 1003
    info.workflow_deadline_epoch_ms = 1004
    info.deduplication_id = "wf-dedup"
    info.priority = 5
    info.queue_partition_key = "wf-partition"
    info.forked_from = "wf-forked-from"
    info.was_forked_from = True
    info.parent_workflow_id = "wf-parent"
    info.dequeued_at = 1005
    info.delay_until_epoch_ms = 1006
    info.completed_at = 1007
    info.attributes = {"customer": "acme", "tier": 1}
    info.schedule_name = "wf-schedule"
    info.application_name = "wf-app"
    info.app_id = "wf-app-id"
    info.recovery_attempts = 2
    return info


def test_workflows_output_maps_every_field() -> None:
    output = p.WorkflowsOutput.from_workflow_information(populated_workflow_status())
    assert output == p.WorkflowsOutput(
        WorkflowUUID="wf-id",
        Status="ENQUEUED",
        WorkflowName="wf-name",
        WorkflowClassName="wf-class",
        WorkflowConfigName="wf-config",
        AuthenticatedUser="wf-user",
        AssumedRole="wf-role",
        AuthenticatedRoles="['wf-role', 'wf-other-role']",
        Input="{'args': (1,), 'kwargs': {'k': 'v'}}",
        Output="wf-output",
        Error="wf-error",
        CreatedAt="1001",
        UpdatedAt="1002",
        QueueName="wf-queue",
        ApplicationVersion="wf-version",
        ExecutorID="wf-executor",
        WorkflowTimeoutMS="1003",
        WorkflowDeadlineEpochMS="1004",
        DeduplicationID="wf-dedup",
        Priority="5",
        QueuePartitionKey="wf-partition",
        ForkedFrom="wf-forked-from",
        WasForkedFrom=True,
        ParentWorkflowID="wf-parent",
        DequeuedAt="1005",
        DelayUntilEpochMS="1006",
        CompletedAt="1007",
        Attributes='{"customer": "acme", "tier": 1}',
        ScheduleName="wf-schedule",
        ApplicationName="wf-app",
    )

    # Conductor consumes the JSON wire format
    response = p.GetWorkflowResponse(
        type=p.MessageType.GET_WORKFLOW, request_id="req", output=output
    )
    assert json.loads(response.to_json())["output"] == asdict(output)


def test_workflows_output_keeps_unset_fields_none() -> None:
    info = populated_workflow_status()
    required = {"workflow_id", "status", "name", "was_forked_from"}
    for attr in list(vars(info)):
        if attr not in required:
            setattr(info, attr, None)
    info.was_forked_from = False

    output = asdict(p.WorkflowsOutput.from_workflow_information(info))
    # Unset fields must stay None, not become the string "None"
    expected = dict.fromkeys(output, None)
    expected.update(
        WorkflowUUID="wf-id",
        Status="ENQUEUED",
        WorkflowName="wf-name",
        WasForkedFrom=False,
    )
    assert output == expected


def test_workflow_steps_maps_every_field() -> None:
    step: StepInfo = {
        "function_id": 3,
        "function_name": "step-name",
        "output": {"answer": 42},
        "error": RuntimeError("step-error"),
        "child_workflow_id": "step-child",
        "started_at_epoch_ms": 2001,
        "completed_at_epoch_ms": 2002,
    }
    output = p.WorkflowSteps.from_step_info(step)
    assert output == p.WorkflowSteps(
        function_id=3,
        function_name="step-name",
        output="{'answer': 42}",
        error="step-error",
        child_workflow_id="step-child",
        started_at_epoch_ms="2001",
        completed_at_epoch_ms="2002",
    )

    response = p.ListStepsResponse(
        type=p.MessageType.LIST_STEPS, request_id="req", output=[output]
    )
    assert json.loads(response.to_json())["output"] == [asdict(output)]


def test_workflow_steps_keeps_unset_fields_none() -> None:
    step: StepInfo = {
        "function_id": 1,
        "function_name": "step-name",
        "output": None,
        "error": None,
        "child_workflow_id": None,
        "started_at_epoch_ms": None,
        "completed_at_epoch_ms": None,
    }
    assert p.WorkflowSteps.from_step_info(step) == p.WorkflowSteps(
        function_id=1,
        function_name="step-name",
        output=None,
        error=None,
        child_workflow_id=None,
        started_at_epoch_ms=None,
        completed_at_epoch_ms=None,
    )
