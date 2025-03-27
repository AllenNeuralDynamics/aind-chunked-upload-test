"""Job to test chunked data. Simply copies from one location to another."""

import logging
import os
import shutil
import sys
from glob import glob
from time import time
from typing import List

from aind_data_transformation.core import GenericEtl, JobResponse

from aind_chunked_upload_test.configs import JobSettings

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


class ChunkedTransformationJob(GenericEtl[JobSettings]):
    """Basic job class."""

    # noinspection PyMissingConstructor
    def __init__(self, job_settings: JobSettings):
        """Class constructor. Overrides parent constructor."""
        self.job_settings = job_settings

    def _extract_list_of_files(self) -> List[str]:
        """Extract a list of files to process."""

        logging.info("Extracting list of files")
        chunk = self.job_settings.chunk
        directory_path = self.job_settings.input_source
        if chunk is None:
            all_paths = glob(
                os.path.join(directory_path, "**", "*"), recursive=True
            )
        else:
            all_paths = glob(
                os.path.join(directory_path, "**", f"*{chunk}*"),
                recursive=True,
            )
        files_and_symlinks = [
            path
            for path in all_paths
            if os.path.isfile(path) or os.path.islink(path)
        ]
        files_and_symlinks.sort()
        return files_and_symlinks

    def _transform_and_load(self, file_list: List[str]) -> None:
        """Transform the data and save it to an output dir."""
        for f in file_list:
            logging.debug(f"Transforming file: {f}")
            f_parts = os.path.splitext(f)
            new_f = f"{f_parts[0]}_transformed{f_parts[1]}"
            new_f_name = os.path.basename(new_f)
            dst = os.path.join(self.job_settings.output_directory, new_f_name)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy(f, dst)
        return None

    def run_job(self) -> JobResponse:
        """Run the job."""
        logging.debug(f"Running job with settings {self.job_settings}")
        start_time = time()
        file_list = self._extract_list_of_files()
        self._transform_and_load(file_list=file_list)
        end_time = time()
        total_time = end_time - start_time
        job_response = JobResponse(
            status_code=200, message=f"Total time for job {total_time}"
        )
        return job_response


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    if len(sys_args) == 2 and sys_args[0] == "--job-settings":
        main_settings = JobSettings.model_validate_json(sys_args[1])
    else:
        main_settings = JobSettings()
    job = ChunkedTransformationJob(job_settings=main_settings)
    job.run_job()
