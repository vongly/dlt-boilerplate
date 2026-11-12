from contextlib import nullcontext
from unittest.mock import Mock

from utils.connections import ssh_tunnel as tunnel_module


def test_starts_tunnel_and_stops_it_when_context_exits(monkeypatch, tmp_path):
    # Make a fake process standing in for the SSH command.
    # poll() returning None means the process is still running.
    fake_process = Mock()
    fake_process.poll.return_value = None

    # Replace subprocess.Popen so the test won't launch a real SSH process.
    # Keep the arguments it receives so we can check the command.
    popen_calls = []

    def fake_popen(command, stdout):
        popen_calls.append((command, stdout))
        return fake_process

    monkeypatch.setattr(tunnel_module.subprocess, "Popen", fake_popen)

    # Pretend connecting to the local forwarded port succeeds.
    # nullcontext() acts like a socket connection that can be used in a "with".
    connection_calls = []

    def fake_create_connection(address, timeout):
        connection_calls.append((address, timeout))
        return nullcontext()

    monkeypatch.setattr(
        tunnel_module.socket,
        "create_connection",
        fake_create_connection,
    )

    # Avoid real delays while the tunnel code waits for readiness.
    monkeypatch.setattr(tunnel_module.time, "sleep", lambda seconds: None)

    # Use a temporary path so the test doesn't depend on your personal SSH key.
    # The fake process won't read or validate this file.
    identity_file = tmp_path / "test_key"

    # Entering the context should start the fake SSH process and check the port.
    with tunnel_module.ssh_tunnel(
        ssh_target="test-user@test-host",
        remote_host="test-db",
        remote_port=5432,
        local_port=15432,
        identity_file=identity_file,
    ):
        # The context body runs only after the code decides the tunnel is ready.
        assert len(popen_calls) == 1
        assert connection_calls == [(("127.0.0.1", 15432), 0.2)]

    # Exiting the context should stop the SSH process and wait for it to end.
    fake_process.terminate.assert_called_once()
    fake_process.wait.assert_called_once_with(timeout=5)

    # Check that SSH was asked to forward the local port to the requested target.
    command, stdout = popen_calls[0]
    assert "-N" in command
    assert "-L" in command
    assert "127.0.0.1:15432:test-db:5432" in command