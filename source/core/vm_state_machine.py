from __future__ import annotations

from statemachine import State, StateMachine

STATE_PENDING = "pending"
STATE_ACTIVE = "active"
STATE_FAILED = "failed"
STATE_DELETING_VM = "deleting_vm"
STATE_DELETING_ISO = "deleting_iso"
STATE_DELETING_NODE = "deleting_node"
STATE_COMPLETED = "completed"

LIFECYCLE_STATES = {
    STATE_PENDING,
    STATE_ACTIVE,
    STATE_FAILED,
    STATE_DELETING_VM,
    STATE_DELETING_ISO,
    STATE_DELETING_NODE,
}
DELETE_STATES = {STATE_DELETING_VM, STATE_DELETING_ISO, STATE_DELETING_NODE}

EVENT_BECAME_ACTIVE = "became_active"
EVENT_BECAME_PENDING = "became_pending"
EVENT_REQUEST_DELETE = "request_delete"
EVENT_INFRA_MISSING = "infra_missing"

EVENT_VM_DONE = "vm_done"
EVENT_VM_RETRY = "vm_retry"
EVENT_ISO_DONE = "iso_done"
EVENT_ISO_RETRY = "iso_retry"
EVENT_NODE_DONE = "node_done"
EVENT_NODE_RETRY = "node_retry"


class VMLifecycleStateMachine(StateMachine):
    pending = State(initial=True, value=STATE_PENDING)
    active = State(value=STATE_ACTIVE)
    failed = State(value=STATE_FAILED)
    deleting_vm = State(value=STATE_DELETING_VM)
    deleting_iso = State(value=STATE_DELETING_ISO)
    deleting_node = State(value=STATE_DELETING_NODE)
    completed = State(final=True, value=STATE_COMPLETED)

    became_active = pending.to(active) | active.to.itself()
    became_pending = active.to(pending) | pending.to.itself()
    mark_failed = pending.to(failed) | active.to(failed) | failed.to.itself()

    request_delete = (
        pending.to(deleting_vm)
        | active.to(deleting_vm)
        | failed.to(deleting_vm)
        | deleting_vm.to.itself()
        | deleting_iso.to.itself()
        | deleting_node.to.itself()
    )

    infra_missing = (
        pending.to(completed)
        | active.to(completed)
        | failed.to(completed)
        | deleting_vm.to(deleting_iso)
        | deleting_iso.to.itself()
        | deleting_node.to.itself()
    )

    vm_done = deleting_vm.to(deleting_iso)
    vm_retry = deleting_vm.to.itself()

    iso_done = deleting_iso.to(deleting_node)
    iso_retry = deleting_iso.to.itself()

    node_done = deleting_node.to(completed)
    node_retry = deleting_node.to.itself()


def is_delete_state(state: str) -> bool:
    return state in DELETE_STATES


def is_lifecycle_state(state: str) -> bool:
    return state in LIFECYCLE_STATES


def _machine_for_state(state: str) -> VMLifecycleStateMachine:
    machine = VMLifecycleStateMachine()
    if state == STATE_PENDING:
        return machine
    if state == STATE_ACTIVE:
        machine.became_active()
        return machine
    if state == STATE_FAILED:
        machine.mark_failed()
        return machine
    if state == STATE_DELETING_VM:
        machine.request_delete()
        return machine
    if state == STATE_DELETING_ISO:
        machine.request_delete()
        machine.vm_done()
        return machine
    if state == STATE_DELETING_NODE:
        machine.request_delete()
        machine.vm_done()
        machine.iso_done()
        return machine
    if state == STATE_COMPLETED:
        machine.request_delete()
        machine.vm_done()
        machine.iso_done()
        machine.node_done()
        return machine
    raise ValueError(f"unsupported lifecycle state: {state}")


def transition_state(state: str, event: str) -> str:
    machine = _machine_for_state(state)
    handler = getattr(machine, event)
    handler()
    return str(machine.current_state.value)
