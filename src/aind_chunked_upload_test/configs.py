"""Configs for job"""

from typing import Optional

from aind_data_transformation.core import BasicJobSettings
from pydantic import Field


class JobSettings(
    BasicJobSettings, cli_parse_args=True, cli_ignore_unknown_args=True
):
    """Basic job settings. Will parse cli args."""

    chunk: Optional[str] = Field(default=None)
    extra_param: Optional[str] = Field(default=None)
