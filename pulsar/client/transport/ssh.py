import os
import subprocess
from typing import (
    List,
    Optional,
    Sequence,
)

SSH_OPTIONS = ['-o', 'StrictHostKeyChecking=no', '-o', 'PreferredAuthentications=publickey', '-o', 'PubkeyAuthentication=yes']


def rsync_get_file(uri_from, uri_to, user, host, port, key, recursive=False, rsync_options=None, ssh_options=None):
    cmd = _rsync_command(
        f'{user}@{host}:{uri_from}',
        uri_to,
        key,
        port,
        recursive=recursive,
        rsync_options=rsync_options,
        ssh_options=ssh_options,
    )
    _call(cmd)


def rsync_post_file(uri_from, uri_to, user, host, port, key, recursive=False, rsync_options=None, ssh_options=None):
    _ensure_dir(uri_to, key, port, user, host, ssh_options=ssh_options)
    cmd = _rsync_command(
        uri_from,
        f'{user}@{host}:{uri_to}',
        key,
        port,
        recursive=recursive,
        rsync_options=rsync_options,
        ssh_options=ssh_options,
    )
    _call(cmd)


def scp_get_file(uri_from, uri_to, user, host, port, key, recursive=False, ssh_options=None):
    cmd = _scp_command(
        f'{user}@{host}:{uri_from}',
        uri_to,
        key,
        port,
        recursive=recursive,
        ssh_options=ssh_options,
    )
    _call(cmd)


def scp_post_file(uri_from, uri_to, user, host, port, key, recursive=False, ssh_options=None):
    _ensure_dir(uri_to, key, port, user, host, ssh_options=ssh_options)
    cmd = _scp_command(
        uri_from,
        f'{user}@{host}:{uri_to}',
        key,
        port,
        recursive=recursive,
        ssh_options=ssh_options,
    )
    _call(cmd)


def _rsync_command(uri_from, uri_to, key, port, recursive=False, rsync_options=None, ssh_options=None):
    cmd = ['rsync', '-e', _ssh_command(key, port, ssh_options=ssh_options)]
    if recursive:
        cmd.append('-r')
    cmd.extend(_option_list(rsync_options))
    cmd.extend([uri_from, uri_to])
    return cmd


def _scp_command(uri_from, uri_to, key, port, recursive=False, ssh_options=None):
    cmd = ['scp', '-P', str(port), '-i', key]
    if recursive:
        cmd.append('-r')
    cmd.extend(effective_ssh_options(ssh_options))
    cmd.extend([uri_from, uri_to])
    return cmd


def _ssh_command(key, port, ssh_options=None):
    return 'ssh -i {} -p {} {}'.format(key, port, ' '.join(effective_ssh_options(ssh_options)))


def effective_ssh_options(ssh_options=None):
    """SSH options for a transfer. A configured (non-empty) list fully replaces
    the Pulsar defaults, with `*defaults*` expanding to them in place. This
    matters because ssh keeps the *first* value of a duplicated -o keyword, so
    options appended after the defaults could never override them."""
    if not ssh_options:
        return list(SSH_OPTIONS)
    options = []
    for option in _option_list(ssh_options):
        if option == '*defaults*':
            options.extend(SSH_OPTIONS)
        else:
            options.append(option)
    return options


def _option_list(options) -> List[str]:
    if options is None:
        return []
    if isinstance(options, str):
        return options.split()
    return [str(option) for option in options]


def _ensure_dir(uri_to, key, port, user, host, ssh_options: Optional[Sequence[str]] = None):
    directory = os.path.dirname(uri_to)
    cmd = ['ssh', '-i', key, '-p', str(port), *effective_ssh_options(ssh_options), f'{user}@{host}', 'mkdir', '-p', directory]
    _call(cmd)


def _call(cmd):
    exit_code = subprocess.check_call(cmd)
    if exit_code != 0:
        raise Exception(f"{cmd[0]} exited with code {exit_code}")


___all__ = [
    'rsync_post_file',
    'rsync_get_file',
    'scp_post_file',
    'scp_get_file'
]
