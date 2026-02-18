from __future__ import annotations

import unittest

from helpers import bootstrap_tests

bootstrap_tests()

from core.vm_state_machine import (
    EVENT_BECAME_ACTIVE,
    EVENT_BECAME_PENDING,
    EVENT_INFRA_MISSING,
    EVENT_ISO_DONE,
    EVENT_ISO_RETRY,
    EVENT_NODE_DONE,
    EVENT_NODE_RETRY,
    EVENT_REQUEST_DELETE,
    EVENT_VM_DONE,
    EVENT_VM_RETRY,
    STATE_ACTIVE,
    STATE_COMPLETED,
    STATE_DELETING_ISO,
    STATE_DELETING_NODE,
    STATE_DELETING_VM,
    STATE_PENDING,
    is_delete_state,
    transition_state,
)


class VmStateMachineTests(unittest.TestCase):
    def test_happy_path(self):
        state = transition_state(STATE_DELETING_VM, EVENT_VM_DONE)
        self.assertEqual(state, STATE_DELETING_ISO)

        state = transition_state(state, EVENT_ISO_DONE)
        self.assertEqual(state, STATE_DELETING_NODE)

        state = transition_state(state, EVENT_NODE_DONE)
        self.assertEqual(state, STATE_COMPLETED)

    def test_retry_transitions(self):
        self.assertEqual(transition_state(STATE_DELETING_VM, EVENT_VM_RETRY), STATE_DELETING_VM)
        self.assertEqual(transition_state(STATE_DELETING_ISO, EVENT_ISO_RETRY), STATE_DELETING_ISO)
        self.assertEqual(transition_state(STATE_DELETING_NODE, EVENT_NODE_RETRY), STATE_DELETING_NODE)

    def test_pending_active_transitions(self):
        self.assertEqual(transition_state(STATE_PENDING, EVENT_BECAME_ACTIVE), STATE_ACTIVE)
        self.assertEqual(transition_state(STATE_ACTIVE, EVENT_BECAME_PENDING), STATE_PENDING)

    def test_request_delete_from_regular_states(self):
        self.assertEqual(transition_state(STATE_PENDING, EVENT_REQUEST_DELETE), STATE_DELETING_VM)
        self.assertEqual(transition_state(STATE_ACTIVE, EVENT_REQUEST_DELETE), STATE_DELETING_VM)

    def test_infra_missing_transitions(self):
        self.assertEqual(transition_state(STATE_PENDING, EVENT_INFRA_MISSING), STATE_COMPLETED)
        self.assertEqual(transition_state(STATE_ACTIVE, EVENT_INFRA_MISSING), STATE_COMPLETED)
        self.assertEqual(transition_state(STATE_DELETING_VM, EVENT_INFRA_MISSING), STATE_DELETING_ISO)
        self.assertEqual(transition_state(STATE_DELETING_ISO, EVENT_INFRA_MISSING), STATE_DELETING_ISO)
        self.assertEqual(transition_state(STATE_DELETING_NODE, EVENT_INFRA_MISSING), STATE_DELETING_NODE)

    def test_is_delete_state(self):
        self.assertTrue(is_delete_state(STATE_DELETING_VM))
        self.assertTrue(is_delete_state(STATE_DELETING_ISO))
        self.assertTrue(is_delete_state(STATE_DELETING_NODE))
        self.assertFalse(is_delete_state("pending"))


if __name__ == "__main__":
    unittest.main()
