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

"""Native terminal failures must not consume a trial through identical restarts."""
from __future__ import annotations

import json

import pytest

from sforge.harness.agent.codex import CodexAgent
from sforge.harness.backend.base import StreamingExecResult
from sforge.harness.config import SForgeConfig
from test_run_agent_cleanup import harness, runner, assert_cleaned  # noqa: F401


@pytest.mark.parametrize('output,exit_code,timed_out,expected', [
    ("ERROR: You've hit your usage limit. Try again later.\ntokens used\n1,234\n", 1, False, 'usage_limit'),
    ("ERROR: You’ve hit your usage limit.\n", 1, False, 'usage_limit'),
    ("ERROR: You've hit your usage limit.\n", 0, False, None),
    ("ERROR: You've hit your usage limit.\n", 1, True, None),
    ("Quoted: ERROR: You've hit your usage limit.\n", 1, False, None),
    ("ERROR: You've hit your usage limit.\nRecovered, working again.\n", 1, False, None),
    ("ERROR: HTTP 429 Too Many Requests\n", 1, False, None),
    ("ERROR: authentication failed\n", 1, False, None),
    ("ERROR: You've hit your usage limit.\ntokens used\nunknown\n", 1, False, None),
])
def test_codex_terminal_diagnostic(output, exit_code, timed_out, expected):
    result = StreamingExecResult(output=output, exit_code=exit_code, timed_out=timed_out)
    assert CodexAgent(SForgeConfig()).resume_block_reason(result) == expected


@pytest.mark.parametrize('hard_quota', [True, False])
def test_real_runner_blocks_only_nonretryable_provider_failure(harness, hard_quota):
    first = StreamingExecResult(
        output="ERROR: You've hit your usage limit.\n" if hard_quota else 'ERROR: HTTP 429\n',
        exit_code=1, elapsed_seconds=2, terminal_observed=True, native_exit_code=1)
    second = StreamingExecResult(output='budget edge', exit_code=124, timed_out=True,
                                 elapsed_seconds=28, terminal_observed=True, native_exit_code=124)
    harness.backend.exec_run_with_timeout.side_effect = [first, second]
    from sforge.harness.task_spec import TaskSpec, WorkSpec, JudgeSpec
    task = TaskSpec(task_id='resume-fixture', name='Resume fixture', base_image='python',
        platform='linux/amd64', cwd='/workspace', submit_paths=['.'], submit_exclude=[],
        work=WorkSpec(specs_dir='', agent_query='Synthetic task', image_tag='fixture'),
        judge=JudgeSpec(eval_cmd='true', eval_timeout=1, parser='pytest', image_tag='fixture'))
    result = runner.run_agent(task, CodexAgent(harness.config), harness.config, harness.backend,
        'resume-run', timeout=30, disable_stop_hook=True, disable_auto_eval=True)
    receipt = json.loads((harness.config.log_dir/'runs/resume-run/resume-fixture/solver_execution.json').read_text())
    assert len(receipt['segments']) == (1 if hard_quota else 2)
    assert receipt['segments'][0]['native_exit_code'] == 1
    assert receipt['segments'][0]['elapsed_seconds'] == 2
    assert receipt['segments'][0].get('resume_block_reason') == ('usage_limit' if hard_quota else None)
    assert result.resume_count == (0 if hard_quota else 1)
    assert result.timed_out is (not hard_quota)
    assert result.archive == b'synthetic-archive'
    assert result.agent_submissions == 1
    assert_cleaned(harness, harness.handle)
