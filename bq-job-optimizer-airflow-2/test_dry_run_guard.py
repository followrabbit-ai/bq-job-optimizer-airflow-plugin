#!/usr/bin/env python3
"""Unit tests for the statement-level dry-run guard and client-identity header."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock

os.environ.setdefault(
    "AIRFLOW_HOME",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "airflow_home"),
)

import rabbit_bq_optimizer_plugin as plugin


class TestRewriteDetection(unittest.TestCase):
    def test_detects_query_text_change(self):
        orig = {"query": {"query": "SELECT 1"}}
        self.assertFalse(plugin._optimized_query_was_rewritten(orig, {"query": {"query": "SELECT 1"}}))
        self.assertTrue(
            plugin._optimized_query_was_rewritten(orig, {"query": {"query": "SET @@reservation='none';\nSELECT 1"}})
        )

    def test_no_query_or_reservation_only_change_is_not_a_rewrite(self):
        # Whole-job reservation assignment leaves the query text untouched.
        orig = {"query": {"query": "SELECT 1"}}
        optimized = {"query": {"query": "SELECT 1"}, "reservation": "projects/p/locations/EU/reservations/r"}
        self.assertFalse(plugin._optimized_query_was_rewritten(orig, optimized))


class TestDryRunGuard(unittest.TestCase):
    def _hook(self, side_effect=None):
        hook = MagicMock()
        hook.project_id = "src-project"
        if side_effect is not None:
            hook.get_client.return_value.query.side_effect = side_effect
        return hook

    def test_valid_rewrite_passes(self):
        optimized = {"query": {"query": "SET @@reservation='none';\nSELECT 1"}}
        self.assertTrue(plugin._rewritten_query_dry_run_ok(self._hook(), optimized, "src-project"))

    def test_invalid_rewrite_falls_back(self):
        optimized = {"query": {"query": "SELECT 'a\nSET @@reservation='none';\nb'"}}
        # A failing dry-run (e.g. syntax error from a mis-split) must return False -> submit the original.
        self.assertFalse(
            plugin._rewritten_query_dry_run_ok(self._hook(side_effect=Exception("Syntax error")), optimized, "src-project")
        )

    def test_client_identity_header_value(self):
        self.assertTrue(plugin.RABBIT_CLIENT_INFO.startswith("rabbit-bq-optimizer-airflow-plugin/"))


if __name__ == "__main__":
    unittest.main()
