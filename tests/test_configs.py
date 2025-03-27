"""Tests for configs module"""

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from aind_chunked_upload_test.configs import JobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"


class TestJobSettings(unittest.TestCase):
    """Tests for JobSettings class"""

    @patch.dict(
        os.environ,
        {
            "TRANSFORMATION_JOB_INPUT_SOURCE": str(TEST_DIR),
            "TRANSFORMATION_JOB_OUTPUT_DIRECTORY": str(TEST_DIR / "output"),
        },
        clear=True,
    )
    def test_basic_construct(self):
        """Tests basic construction"""
        job_settings = JobSettings(chunk="a")
        self.assertEqual(str(TEST_DIR), job_settings.input_source)
        self.assertEqual(
            str(TEST_DIR / "output"), job_settings.output_directory
        )
        self.assertEqual("a", job_settings.chunk)
        self.assertIsNone(job_settings.extra_param)


if __name__ == "__main__":
    unittest.main()
