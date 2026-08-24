"""Airflow 2.x / 3.x import compatibility shim.

Airflow 3.0 introduced ``airflow.sdk`` as the public namespace for DAG
and TaskGroup. Airflow 2.x has the same classes at their pre-3.0
locations:

* ``airflow.models.dag.DAG``
* ``airflow.utils.task_group.TaskGroup``

This shim picks the right import path based on the installed Airflow
version so the lib runs unchanged on MWAA 2.10.x as well as local
Airflow 3.x. Callers should import ``DAG`` / ``TaskGroup`` from here
rather than from ``airflow.sdk`` directly.
"""

from __future__ import annotations

try:  # Airflow 3.x
    from airflow.sdk import DAG, TaskGroup
except ImportError:  # Airflow 2.x fallback
    from airflow.models.dag import DAG  # type: ignore[no-redef]
    from airflow.utils.task_group import TaskGroup  # type: ignore[no-redef]

__all__ = ["DAG", "TaskGroup"]
