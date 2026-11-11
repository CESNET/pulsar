import json
from contextlib import contextmanager
from unittest import mock

from galaxy.util.bunch import Bunch

from pulsar.client import action_mapper
from pulsar.client.action_mapper import (
    UNSET_ACTION_KWD,
    FileActionMapper,
    from_dict,
)
from pulsar.client.job_directory import RemoteJobDirectory
from pulsar.client.staging.down import ResultsCollector
from pulsar.client.staging.models import (
    ClientOutputs,
    PulsarOutputs,
)
from pulsar.client.staging.up import (
    StageDirectoryType,
    TransferTracker,
)
from pulsar.client.transport import ssh
from pulsar.client.util import PathHelper


def test_endpoint_validation():
    client = _min_client("remote_transfer")
    mapper = FileActionMapper(client)
    exception_found = False
    try:
        mapper.action({'path': '/opt/galaxy/tools/filters/catWrapper.py'}, 'input')
    except Exception as e:
        exception_found = True
        assert "files_endpoint" in str(e)
    assert exception_found


def test_ssh_key_validation():
    client = _min_client("remote_rsync_transfer")
    mapper = FileActionMapper(client)
    exception_found = False
    try:
        mapper.action({'path': '/opt/galaxy/tools/filters/catWrapper.py'}, 'input')
    except Exception as e:
        exception_found = True
        assert "ssh_key" in str(e)
    assert exception_found


def test_ssh_key_defaults():
    client = _client("remote_rsync_transfer")
    mapper = FileActionMapper(client)
    action = mapper.action({'path': '/opt/galaxy/tools/filters/catWrapper.py'}, 'input')
    action.to_dict()


def test_rsync_transfer_action_options():
    # Options configured per-path survive the to_dict/from_dict round trip to
    # the Pulsar server and end up on the rsync command line.
    mapper = FileActionMapper(config=_rsync_config(paths=[{
        "action": "remote_rsync_transfer",
        "path_types": "input",
        "recursive": True,
        "rsync_options": ["-z", "--bwlimit=1000"],
        "ssh_options": ["-o", "Compression=yes"],
    }]))
    action = mapper.action({"path": "/data/in"}, "input")
    server_action = from_dict(_inject_ssh_key(action.to_dict()))
    with _captured_calls() as captured:
        server_action.write_to_path("/staged/in")
    cmd = captured[0]
    assert cmd[0] == "rsync"
    assert "-r" in cmd
    assert "-z" in cmd
    assert "--bwlimit=1000" in cmd
    assert "-o Compression=yes" in cmd[2]  # appended to the ssh -e command


def test_ssh_options_default_when_unset():
    with _captured_calls() as captured:
        ssh.scp_get_file("/remote/f", "/local/f", "user", "host", 22, "key")
    cmd = captured[0]
    assert "StrictHostKeyChecking=no" in cmd
    assert "PreferredAuthentications=publickey" in cmd
    assert "PubkeyAuthentication=yes" in cmd


def test_ssh_options_replace_defaults():
    # A configured list replaces the Pulsar defaults...
    with _captured_calls() as captured:
        ssh.scp_get_file("/remote/f", "/local/f", "user", "host", 22, "key",
                         ssh_options=["-o", "PreferredAuthentications=gssapi-with-mic,password"])
    cmd = captured[0]
    assert "PreferredAuthentications=gssapi-with-mic,password" in cmd
    assert "StrictHostKeyChecking=no" not in cmd
    assert "PreferredAuthentications=publickey" not in cmd

    # ...`*defaults*` expands them in place, and since ssh keeps the first -o
    # of a kind the replacement has to precede the token.
    options = ssh.effective_ssh_options(["-o", "StrictHostKeyChecking=yes", "*defaults*"])
    assert options.index("StrictHostKeyChecking=yes") < options.index("StrictHostKeyChecking=no")
    assert options.count("StrictHostKeyChecking=yes") == 1
    assert "PreferredAuthentications=publickey" in options


def test_server_transfer_defaults():
    # app.yml (Pulsar server) defaults apply to actions that leave the
    # options unset...
    mapper = FileActionMapper(config=_rsync_config(), transfer_defaults={
        "recursive": True,
        "rsync_options": "-avz",
        "ssh_options": None,
    })
    action = mapper.action({"path": "/data/out"}, "output")
    with _captured_calls() as captured:
        action.write_from_path("/pulsar/out")
    cmd = captured[-1]
    assert "-r" in cmd
    assert "-avz" in cmd

    # ...but per-path options win over the server defaults.
    recursive_off_mapper = FileActionMapper(
        config=_rsync_config(paths=[{"action": "remote_rsync_transfer", "path_types": "output", "recursive": False}]),
        transfer_defaults={"recursive": True},
    )
    action = recursive_off_mapper.action({"path": "/data/out"}, "output")
    with _captured_calls() as captured:
        action.write_from_path("/pulsar/out")
    assert "-r" not in captured[-1]


def test_transfer_defaults_do_not_break_legacy_actions():
    # Action dicts produced by older clients carry none of the new keys.
    legacy = {"action_type": "remote_rsync_transfer", "path": "/x", "source": {"path": "/x"},
              "ssh_user": "user", "ssh_host": "host", "ssh_port": 22, "ssh_key": "KEY"}
    action = from_dict(dict(legacy))
    action_mapper.apply_transfer_defaults(action, {"recursive": False, "ssh_options": None, "rsync_options": None})
    with _captured_calls() as captured:
        action.write_to_path("/y")
    cmd = captured[0]
    assert cmd[0] == "rsync"
    assert "-r" not in cmd


def test_recursive_extra_files_single_action(tmp_path):
    # With recursive staging on, a dataset extra_files directory collapses to
    # one remote action (recursive, whole-directory) instead of one per file.
    extra_files = tmp_path / "dataset_1_files"
    (extra_files / "sub").mkdir(parents=True)
    (extra_files / "a.txt").write_text("a")
    (extra_files / "b.txt").write_text("b")
    (extra_files / "sub" / "c.txt").write_text("c")

    tracker = _tracker(staging_recursive=True)
    tracker.handle_transfer_directory(
        "input",
        action_source={"path": str(extra_files), "object_store_ref": None},
        mode=StageDirectoryType.WHOLE_DIRECTORY,
    )
    assert len(tracker.remote_staging_actions) == 1
    staged_action = tracker.remote_staging_actions[0]["action"]
    assert tracker.remote_staging_actions[0]["name"] == "dataset_1_files"
    assert staged_action["recursive"] is True
    assert staged_action["directory"] is True
    # Every file (including in subdirs) still gets a path rewrite.
    assert tracker.file_renames[str(extra_files / "a.txt")] == "/staging/inputs/dataset_1_files/a.txt"
    assert tracker.file_renames[str(extra_files / "sub" / "c.txt")] == "/staging/inputs/dataset_1_files/sub/c.txt"

    # The whole-directory action transfers with -r and a trailing slash so
    # retries stay idempotent (contents into the destination, not nested).
    server_action = from_dict(_inject_ssh_key(dict(staged_action)))
    with _captured_calls() as captured:
        server_action.write_to_path("/staging/inputs/dataset_1_files")
    cmd = captured[0]
    assert cmd[0] == "rsync"
    assert "-r" in cmd
    remote_source = cmd[-2]
    local_dest = cmd[-1]
    assert remote_source.endswith("dataset_1_files/")
    assert local_dest == "/staging/inputs/dataset_1_files"


def test_non_recursive_extra_files_one_action_per_file(tmp_path):
    extra_files = tmp_path / "dataset_1_files"
    extra_files.mkdir()
    (extra_files / "a.txt").write_text("a")
    (extra_files / "b.txt").write_text("b")

    tracker = _tracker(staging_recursive=False)
    tracker.handle_transfer_directory(
        "input",
        action_source={"path": str(extra_files), "object_store_ref": None},
        mode=StageDirectoryType.WHOLE_DIRECTORY,
    )
    # Default behavior preserved: the directory is walked, one action per file.
    assert len(tracker.remote_staging_actions) == 2


def _tracker(staging_recursive):
    action_mapper_instance = FileActionMapper(config=_rsync_config())
    job_directory = RemoteJobDirectory("/staging", remote_id=None, remote_sep="/")
    return TransferTracker(
        client=None,
        path_helper=PathHelper("/"),
        action_mapper=action_mapper_instance,
        job_inputs=None,
        rewrite_paths=True,
        job_directory=job_directory,
        staging_recursive=staging_recursive,
    )


class _RecordingOutputCollector:
    """Minimal output collector that records collect_output calls and reports success."""

    def __init__(self):
        self.calls = []

    def collect_output(self, results_collector, output_type, action, name):
        self.calls.append((output_type, getattr(action, "action_type", None), name, getattr(action, "directory", UNSET_ACTION_KWD)))
        return True


def _results_collector(transfer_defaults):
    mapper = FileActionMapper(config=_rsync_config(default_action="remote_rsync_transfer"), transfer_defaults=transfer_defaults)
    collector = _RecordingOutputCollector()
    client_outputs = ClientOutputs(working_directory="/galaxy/wd", output_files=["/galaxy/outputs/out1.dat"])
    pulsar_outputs = PulsarOutputs(
        [],
        ["out1.dat", "out1_files/a.txt", "out1_files/sub/b.txt", "out1_files/c.txt"],
        [],
        [],
        remote_separator="/",
    )
    return ResultsCollector(collector, mapper, client_outputs, pulsar_outputs), collector


def test_output_extra_files_recursive_single_collection():
    # Pulsar server with staging_recursive: the output dataset's extra_files
    # come back as ONE whole-directory collection, not one per file.
    collector_rc, collector = _results_collector({"recursive": True})
    collector_rc.collect()
    directory_calls = [c for c in collector.calls if c[3] is True]
    assert len(directory_calls) == 1
    assert directory_calls[0][2] == "out1_files"
    collected_names = [c[2] for c in collector.calls]
    assert "out1_files/a.txt" not in collected_names
    assert "out1_files/sub/b.txt" not in collected_names
    assert "out1_files/c.txt" not in collected_names


def test_output_extra_files_non_recursive_one_per_file():
    # Without recursive staging the per-file collection is preserved.
    collector_rc, collector = _results_collector({})
    collector_rc.collect()
    collected_names = [c[2] for c in collector.calls]
    assert "out1_files/a.txt" in collected_names
    assert "out1_files/sub/b.txt" in collected_names
    assert "out1_files/c.txt" in collected_names
    assert [c for c in collector.calls if c[3] is True] == []


def test_output_extra_files_discovered_directory_recursive():
    # discovered_datasets (galaxy.json extra_files reference) also collapse to
    # one whole-directory collection when recursive staging is on.
    transfer_defaults = {"recursive": True}
    mapper = FileActionMapper(config=_rsync_config(default_action="remote_rsync_transfer"), transfer_defaults=transfer_defaults)
    collector = _RecordingOutputCollector()
    galaxy_json = json.dumps({"collections": {"col1": {"type": "list", "extra_files": "col1_extra"}}})
    client_outputs = ClientOutputs(working_directory="/galaxy/wd", output_files=[])
    pulsar_outputs = PulsarOutputs(
        ["col1_extra/x.txt", "col1_extra/sub/y.txt"],
        [],
        [],
        [],
        remote_separator="/",
        realized_dynamic_file_sources=[{"contents": galaxy_json, "type": "galaxy", "path": "galaxy.json"}],
    )
    ResultsCollector(collector, mapper, client_outputs, pulsar_outputs).collect()
    directory_calls = [c for c in collector.calls if c[3] is True]
    assert len(directory_calls) == 1
    assert directory_calls[0][2] == "col1_extra"
    collected_names = [c[2] for c in collector.calls]
    assert "col1_extra/x.txt" not in collected_names


def test_rsync_directory_post_is_idempotent():
    # The server stages a whole directory out with -r and a trailing slash on
    # the Pulsar source (so retries merge instead of nesting), the client
    # destination keeps its name.
    mapper = FileActionMapper(config=_rsync_config(default_action="remote_rsync_transfer"), transfer_defaults={"recursive": True})
    action = mapper.action({"path": "/galaxy/outputs/out1_files"}, "output")
    action.recursive = True
    action.directory = True
    with _captured_calls() as captured:
        action.write_from_path("/pulsar/staging/outputs/out1_files")
    cmd = captured[-1]  # captured[0] is the ensure_dir mkdir
    assert "-r" in cmd
    # Pulsar source keeps a trailing slash; Galaxy destination (remote here) does not.
    assert cmd[-2] == "/pulsar/staging/outputs/out1_files/"
    assert cmd[-1] == "user@host:/galaxy/outputs/out1_files"


def test_scp_transfer_recursive_and_ssh_options():
    mapper = FileActionMapper(
        config=_rsync_config(default_action="remote_scp_transfer"),
        transfer_defaults={"recursive": True, "ssh_options": ["-o", "ProxyJump=jump.example"]},
    )
    action = mapper.action({"path": "/data/out"}, "output")
    with _captured_calls() as captured:
        action.write_from_path("/pulsar/out")
    cmd = captured[-1]
    assert cmd[0] == "scp"
    assert "-r" in cmd
    assert "-o" in cmd
    assert "ProxyJump=jump.example" in cmd


def _rsync_config(default_action="remote_rsync_transfer", paths=None):
    return {
        "default_action": default_action,
        "ssh_key": "KEY",
        "ssh_user": "user",
        "ssh_host": "host",
        "ssh_port": 2222,
        "paths": paths or [],
    }


def _inject_ssh_key(action_dict):
    # The Pulsar server injects the key into setup actions from the action
    # mapper config - it is never serialized with the action itself.
    action_dict["ssh_key"] = "KEY"
    return action_dict


@contextmanager
def _captured_calls():
    commands = []
    with mock.patch.object(ssh, "_call", lambda cmd: commands.append(cmd)):
        yield commands


def _min_client(default_action):
    """Minimal client, missing properties for certain actions."""
    mock_client = Bunch(
        default_file_action=default_action,
        action_config_path=None,
        files_endpoint=None,
        ssh_key=None,
    )
    return mock_client


def _client(default_action):
    mock_client = Bunch(
        default_file_action=default_action,
        action_config_path=None,
        files_endpoint="http://localhost",
        ssh_key="12345",
    )
    return mock_client
