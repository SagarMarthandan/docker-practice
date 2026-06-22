"""Shared test configuration.

Installs lightweight Airflow stubs so DAG modules can be imported without
a running Airflow instance.  The stubs turn @dag-decorated functions into
no-ops at call time (they still define their inner tasks) and @task
functions into callables that return a sentinel instead of executing.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# ── repo paths ──────────────────────────────────────────────────────────────
_repo = Path(__file__).resolve().parent.parent
for sub in ("dags", "pipeline"):
    _dir = _repo / sub
    if _dir.is_dir() and str(_dir) not in sys.path:
        sys.path.insert(0, str(_dir))


# ── Airflow stubs ───────────────────────────────────────────────────────────

_SENTINEL = "STUB_XCOM"


def _install_airflow_stubs():
    stubs = {}

    def _make(name):
        mod = types.ModuleType(name)
        stubs[name] = mod
        return mod

    _make("airflow")
    _make("airflow.decorators")
    _make("airflow.models")
    _make("airflow.models.param")
    _make("airflow.providers")
    _make("airflow.providers.postgres")
    _make("airflow.providers.postgres.hooks")
    _make("airflow.providers.postgres.hooks.postgres")
    _make("airflow.providers.google")
    _make("airflow.providers.google.cloud")
    _make("airflow.providers.google.cloud.hooks")
    _make("airflow.providers.google.cloud.hooks.gcs")
    _make("airflow.providers.google.cloud.hooks.bigquery")
    _make("google")
    _make("google.cloud")
    _make("google.cloud.bigquery")
    _make("requests")

    # requests stub
    stubs["requests"].get = MagicMock(return_value=MagicMock(
        json=MagicMock(return_value={}),
        raise_for_status=MagicMock(),
    ))

    # @dag — the decorated function becomes a no-op when called
    def _dag_decorator(**kwargs):
        def wrapper(fn):
            def noop(*a, **kw):
                pass
            # preserve the original function for introspection
            noop._original = fn
            noop.__name__ = fn.__name__
            return noop
        return wrapper

    # @task — the decorated function returns a sentinel when called,
    # allowing DAG wiring code (e.g. `path = extract()`) to execute
    # without hitting real logic.
    class _TaskStub:
        """Wraps a function so calling it returns _SENTINEL."""
        def __init__(self, fn):
            self._fn = fn
            self.__name__ = fn.__name__

        def __call__(self, *a, **kw):
            return _SENTINEL

        def __rrshift__(self, other):
            return self

        def __rshift__(self, other):
            return other

    def _task_decorator(fn):
        return _TaskStub(fn)

    stubs["airflow.decorators"].dag = _dag_decorator
    stubs["airflow.decorators"].task = _task_decorator

    # Param
    class _Param:
        def __init__(self, default=None, **kw):
            self.default = default

    stubs["airflow.models.param"].Param = _Param

    # Variable stub
    class _Variable:
        _store: dict = {}
        @classmethod
        def get(cls, key, default_var=None):
            return cls._store.get(key, default_var or key)

    stubs["airflow.models"].Variable = _Variable

    # Hook stubs
    stubs["airflow.providers.postgres.hooks.postgres"].PostgresHook = MagicMock
    stubs["airflow.providers.google.cloud.hooks.gcs"].GCSHook = MagicMock
    stubs["airflow.providers.google.cloud.hooks.bigquery"].BigQueryHook = MagicMock

    for name, mod in stubs.items():
        sys.modules[name] = mod


_install_airflow_stubs()
