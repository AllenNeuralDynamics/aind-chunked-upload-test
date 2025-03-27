"""Tests for transform_data_job module"""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_chunked_upload_test.configs import JobSettings
from aind_chunked_upload_test.transform_data_job import (
    ChunkedTransformationJob,
)

DATA_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / "resources"
    / "dummy_data"
)


class TestChunkedTransformationJob(unittest.TestCase):
    """Tests for ChunkedTransformationJob class"""

    @classmethod
    def setUpClass(cls):
        """Set up job settings that can be used across all tests"""
        job_settings = JobSettings(
            input_source=str(DATA_DIR / "OnixEphys"),
            output_directory=str(DATA_DIR / "ephys"),
        )
        cls.job_settings = job_settings

    @patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}, clear=True)
    def test_extract_list_of_files_no_chunk(self):
        """Tests file list is created correctly when chunk is None"""
        job = ChunkedTransformationJob(job_settings=self.job_settings)
        with self.assertLogs(level="DEBUG") as captured:
            list_of_files = job._extract_list_of_files()
        expected_list_of_files = [
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_AmplifierData_2025-01-31T19-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_AmplifierData_2025-01-31T20-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_AmplifierData_2025-01-31T21-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_Clock_2025-01-31T19-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_Clock_2025-01-31T21-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_Clock_2025-01-31T20-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_HubClock_2025-01-31T19-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_HubClock_2025-01-31T20-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_HubClock_2025-01-31T21-00-00.bin"
            ),
        ]
        expected_list_of_files.sort()
        expected_log_output = ["INFO:root:Extracting list of files"]
        self.assertEqual(expected_list_of_files, list_of_files)
        self.assertEqual(expected_log_output, captured.output)

    @patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}, clear=True)
    def test_extract_list_of_files_with_chunk(self):
        """Tests file list is created correctly when chunk is set"""
        job_settings = self.job_settings.model_copy(
            deep=True, update={"chunk": "2025-01-31T19-00-00"}
        )
        job = ChunkedTransformationJob(job_settings=job_settings)
        with self.assertLogs(level="DEBUG") as captured:
            list_of_files = job._extract_list_of_files()
        expected_list_of_files = [
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_AmplifierData_2025-01-31T19-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_Clock_2025-01-31T19-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_HubClock_2025-01-31T19-00-00.bin"
            ),
        ]
        expected_list_of_files.sort()
        expected_log_output = ["INFO:root:Extracting list of files"]
        self.assertEqual(expected_list_of_files, list_of_files)
        self.assertEqual(expected_log_output, captured.output)

    @patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}, clear=True)
    @patch("os.makedirs")
    @patch("shutil.copy")
    def test_transform_and_load(
        self, mock_copy: MagicMock, mock_make_dirs: MagicMock
    ):
        """Tests transform_and_load outputs correctly"""
        job_settings = self.job_settings.model_copy(
            deep=True, update={"chunk": "2025-01-31T19-00-00"}
        )
        job = ChunkedTransformationJob(job_settings=job_settings)
        list_of_files = [
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_AmplifierData_2025-01-31T19-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_Clock_2025-01-31T19-00-00.bin"
            ),
            str(
                DATA_DIR
                / "OnixEphys"
                / "OnixEphys_HubClock_2025-01-31T19-00-00.bin"
            ),
        ]

        expected_mkdir_calls = [
            call(
                self.job_settings.output_directory,
                exist_ok=True,
            ),
            call(
                self.job_settings.output_directory,
                exist_ok=True,
            ),
            call(
                self.job_settings.output_directory,
                exist_ok=True,
            ),
        ]
        onix_path = DATA_DIR / "OnixEphys"
        dst_path = self.job_settings.output_directory
        expected_copy_calls = [
            call(
                os.path.join(
                    onix_path,
                    "OnixEphys_AmplifierData_2025-01-31T19-00-00.bin",
                ),
                os.path.join(
                    dst_path,
                    (
                        "OnixEphys_AmplifierData_2025-01-31T19-00-00"
                        "_transformed.bin"
                    ),
                ),
            ),
            call(
                os.path.join(
                    onix_path, "OnixEphys_Clock_2025-01-31T19-00-00.bin"
                ),
                os.path.join(
                    dst_path,
                    "OnixEphys_Clock_2025-01-31T19-00-00_transformed.bin",
                ),
            ),
            call(
                os.path.join(
                    onix_path, "OnixEphys_HubClock_2025-01-31T19-00-00.bin"
                ),
                os.path.join(
                    dst_path,
                    "OnixEphys_HubClock_2025-01-31T19-00-00_transformed.bin",
                ),
            ),
        ]
        log_out_paths = [
            os.path.join(
                onix_path, "OnixEphys_AmplifierData_2025-01-31T19-00-00.bin"
            ),
            os.path.join(onix_path, "OnixEphys_Clock_2025-01-31T19-00-00.bin"),
            os.path.join(
                onix_path, "OnixEphys_HubClock_2025-01-31T19-00-00.bin"
            ),
        ]
        expected_log_output = [
            f"DEBUG:root:Transforming file: " f"{log_out_paths[0]}",
            f"DEBUG:root:Transforming file: {log_out_paths[1]}",
            f"DEBUG:root:Transforming file: {log_out_paths[2]}",
        ]

        with self.assertLogs(level="DEBUG") as captured:
            job._transform_and_load(file_list=list_of_files)
        self.assertEqual(expected_mkdir_calls, mock_make_dirs.mock_calls)
        self.assertEqual(expected_copy_calls, mock_copy.mock_calls)
        self.assertEqual(expected_log_output, captured.output)

    @patch.dict(os.environ, {"LOG_LEVEL": "INFO"}, clear=True)
    @patch(
        "aind_chunked_upload_test.transform_data_job.ChunkedTransformationJob"
        "._transform_and_load"
    )
    def test_run_job(self, mock_transform_and_load: MagicMock):
        """Tests all functions are called in run_job method"""
        job_settings = self.job_settings.model_copy(
            deep=True, update={"chunk": "2025-01-31T21-00-00"}
        )
        job = ChunkedTransformationJob(job_settings=job_settings)
        with self.assertLogs(level="INFO") as captured:
            job.run_job()
        onix_path = DATA_DIR / "OnixEphys"
        expected_mock_transform_and_load_calls = [
            call(
                file_list=[
                    os.path.join(
                        onix_path,
                        "OnixEphys_AmplifierData_2025-01-31T21-00-00.bin",
                    ),
                    os.path.join(
                        onix_path, "OnixEphys_Clock_2025-01-31T21-00-00.bin"
                    ),
                    os.path.join(
                        onix_path, "OnixEphys_HubClock_2025-01-31T21-00-00.bin"
                    ),
                ]
            )
        ]
        expected_log_ouptut = ["INFO:root:Extracting list of files"]

        self.assertEqual(
            expected_mock_transform_and_load_calls,
            mock_transform_and_load.mock_calls,
        )
        self.assertEqual(expected_log_ouptut, captured.output)


if __name__ == "__main__":
    unittest.main()
