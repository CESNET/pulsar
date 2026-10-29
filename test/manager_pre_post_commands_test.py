"""Tests for per-manager pre/post job commands (app.yml ``job_pre_commands`` /
``job_post_commands`` and the legacy ``instrument_*`` aliases)."""

from pulsar import manager_factory
from pulsar.managers.base import normalize_commands
from pulsar.managers.util.job_script import job_script
from .test_utils import minimal_app_for_managers


class _FakeInstrumenter:
    """Stands in for a job-instrumenter (job metrics) writing the same slots."""

    def pre_execute_commands(self, job_directory):
        return "hostname -f > host"

    def post_execute_commands(self, job_directory):
        return "uname > u"


def _build(conf):
    app = minimal_app_for_managers()
    managers = manager_factory.build_managers(app, conf)
    return managers


def test_normalize_commands():
    assert normalize_commands(None) == ""
    assert normalize_commands("") == ""
    assert normalize_commands("a\nb") == "a\nb"
    assert normalize_commands(["a", "b", "", "  "]) == "a\nb"


def test_per_manager_and_alias_and_global_default():
    conf = {
        # top-level (global) default, set via the primary key...
        "job_pre_commands": "echo GLOBAL",
        "managers": {
            "inherits": {"type": "queued_python"},
            "primary_override": {"type": "queued_python", "job_pre_commands": ["echo A", "echo B"]},
            # ...and a per-manager override using the legacy alias - must win.
            "alias_override": {"type": "queued_python", "instrument_pre_commands": "echo ALIAS"},
            "cleared": {"type": "queued_python", "job_pre_commands": ""},
        },
    }
    managers = _build(conf)
    try:
        assert managers["inherits"]._proxied_manager.job_pre_commands == "echo GLOBAL"
        assert managers["primary_override"]._proxied_manager.job_pre_commands == "echo A\necho B"
        assert managers["alias_override"]._proxied_manager.job_pre_commands == "echo ALIAS"
        assert managers["cleared"]._proxied_manager.job_pre_commands == ""
    finally:
        for manager in managers.values():
            manager.shutdown()


def test_configured_commands_render_into_job_script():
    managers = _build({"managers": {"m": {"type": "queued_python", "job_pre_commands": "echo PRE", "job_post_commands": "echo POST"}}})
    try:
        manager = managers["m"]._proxied_manager
        # Inject a job-instrumenter so we can assert metrics coexist with the
        # configured commands in the generated script.
        manager.job_metrics = type("JM", (), {"default_job_instrumenter": _FakeInstrumenter()})()
        job_id = "prepost"
        manager._setup_job_directory(job_id)
        script_path = manager._setup_job_file(job_id, "echo THE_JOB")
        script = open(script_path).read()
    finally:
        for manager in managers.values():
            manager.shutdown()

    # Configured pre runs before the instrumenter's, both before the command;
    # instrumenter post before configured post, both after the command.
    assert script.index("echo PRE") < script.index("hostname -f") < script.index("echo THE_JOB")
    assert script.index("uname") < script.index("echo POST")


def test_job_script_merges_without_dropping_metrics():
    base = {"working_directory": "wd", "metadata_directory": "md", "command": "THECOMMAND"}
    merged = job_script(job_instrumenter=_FakeInstrumenter(), instrument_pre_commands="echo pre", **base)
    assert "echo pre" in merged and "hostname -f > host" in merged
    # No configured value -> behaviour is exactly the instrumenter's (no regression).
    metrics_only = job_script(job_instrumenter=_FakeInstrumenter(), **base)
    assert "hostname -f > host" in metrics_only and "uname > u" in metrics_only
