from contextlib import contextmanager
from unittest import mock

from galaxy.util.bunch import Bunch

from pulsar.client import action_mapper
from pulsar.client.action_mapper import (
    FileActionMapper,
    from_dict,
)
from pulsar.client.transport import ssh


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
