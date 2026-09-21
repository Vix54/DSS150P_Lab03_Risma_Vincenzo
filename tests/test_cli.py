import pytest

from src.cli import run_stage
from src.common.errors import PipelineError


def test_pipeline_errors_pass_through_unchanged():
    def failing(_args):
        raise PipelineError('extract', 'missing source files')

    with pytest.raises(PipelineError) as raised:
        run_stage('extract', failing, None)
    assert raised.value.stage == 'extract'
    assert str(raised.value) == '[extract] missing source files'


def test_unexpected_errors_are_wrapped_with_the_stage_name_and_keep_their_cause():
    def failing(_args):
        raise ValueError('bad value')

    with pytest.raises(PipelineError) as raised:
        run_stage('load', failing, None)
    assert raised.value.stage == 'load'
    assert 'unexpected ValueError: bad value' in str(raised.value)
    assert isinstance(raised.value.__cause__, ValueError)
