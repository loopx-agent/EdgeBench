# Copyright (c) 2026 ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Real owned execs with narrowly injected disconnect/read/completion faults."""

import errno

import pytest

from sforge.harness import docker_utils
from test_docker_exec_lifecycle import container, execute  # Reuse isolated Docker fixtures.


def wrap_stream(monkeypatch, container, *, close_errno=errno.ENOTCONN, read_error=False, truncate=False):
    start = container.client.api.exec_start

    class Stream:
        def __init__(self, actual):
            self.actual = actual

        def __iter__(self):
            for chunk in self.actual:
                yield chunk
                if read_error:
                    raise OSError(errno.ENOTCONN, "injected read disconnect")
                if truncate:
                    return

        def close(self):
            try:
                self.actual.close()
            except OSError as error:
                if error.errno != errno.ENOTCONN:
                    raise
            raise OSError(close_errno, "injected close disconnect")

    def opened(exec_id, **options):
        actual = start(exec_id, **options)
        return Stream(actual) if options.get("stream") else actual

    monkeypatch.setattr(container.client.api, "exec_start", opened)


def test_complete_output_disconnected_close_keeps_verified_exit(container, execute, monkeypatch):
    wrap_stream(monkeypatch, container)
    assert execute(["bash", "-c", "printf COMPLETE; exit 17"], timeout=5)[:3] == ("COMPLETE", 17, False)


@pytest.mark.parametrize("read_error,close_errno", [(True, errno.ENOTCONN), (False, errno.EIO)])
def test_read_disconnect_or_other_close_error_is_not_completion(
    container, execute, monkeypatch, read_error, close_errno,
):
    wrap_stream(monkeypatch, container, read_error=read_error, close_errno=close_errno)
    with pytest.raises(OSError) as caught:
        execute(["printf", "COMPLETE"], timeout=5)
    assert caught.value.errno == (errno.ENOTCONN if read_error else errno.EIO)


def test_disconnected_close_does_not_replace_shutdown_receipt(container, execute, monkeypatch):
    wrap_stream(monkeypatch, container)
    monkeypatch.setattr(docker_utils, "_EXEC_SUPERVISOR", "exit 0")
    with pytest.raises(RuntimeError, match="shutdown"):
        execute(["true"], timeout=5)


def test_early_eof_and_disconnected_close_do_not_certify_live_process(container, execute, monkeypatch):
    wrap_stream(monkeypatch, container, truncate=True)
    monkeypatch.setattr(docker_utils, "_EXEC_SHUTDOWN_GRACE", 1)
    monkeypatch.setattr(docker_utils, "_EXEC_DRAIN_GRACE", 1)
    with pytest.raises(RuntimeError, match="stop could not be confirmed"):
        execute(["bash", "-c", "printf READY; sleep 60"], timeout=5)
