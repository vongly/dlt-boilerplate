from pathlib import Path
from contextlib import contextmanager
import socket
import subprocess
import time


@contextmanager
def ssh_tunnel(
    ssh_target,
    remote_host,
    remote_port,
    local_port,
    identity_file,
    ):

    identity_file = Path(identity_file)
    process = subprocess.Popen(
        [
            "ssh",
            "-N",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "IdentitiesOnly=yes",
            "-i", str(identity_file),
            "-L", f"127.0.0.1:{local_port}:{remote_host}:{remote_port}",
            ssh_target,
        ],
        stdout=subprocess.DEVNULL,
    )

    try:
        for _ in range(50):
            if process.poll() is not None:
                raise RuntimeError("SSH tunnel exited before it was ready")
            try:
                with socket.create_connection(("127.0.0.1", local_port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            raise TimeoutError("Timed out waiting for the SSH tunnel")

        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()