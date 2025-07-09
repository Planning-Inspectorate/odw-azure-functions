import os
from typing import Set
import filecmp


class TestingUtility():
    @classmethod
    def get_all_files_under_directory(cls, directory: str) -> Set[str]:
        """
            Deeply retrieve all files contained within the folder
        """
        subfolders_and_files = os.listdir(directory)
        files = {f"{directory}/{x}" for x in subfolders_and_files if os.path.isfile(f"{directory}/{x}")}
        for subfolder in [x for x in subfolders_and_files if not os.path.isfile(f"{directory}/{x}")]:
            files = files.union(cls.get_all_files_under_directory(f"{directory}/{subfolder}"))
        return files
    
    @classmethod
    def compare_directories(cls, directory_a: str, directory_b: str) -> bool:
        """
            Check that the content of the two directories is the same
        """
        directory_a_files = {
            x.replace(f"{directory_a}/", "", 1)
            for x in cls.get_all_files_under_directory(directory_a)
        }
        directory_b_files = {
            x.replace(f"{directory_b}/", "", 1)
            for x in cls.get_all_files_under_directory(directory_b)
        }
        in_directory_a_but_not_b = directory_a_files - directory_b_files
        in_directory_b_but_not_a = directory_b_files - directory_a_files
        assert not in_directory_a_but_not_b, (
            f"Mismatch between directories. Directory '{directory_a}' contains the following extra files: {in_directory_a_but_not_b}"
        )
        assert not in_directory_b_but_not_a, (
            f"Mismatch between directories. Directory '{directory_b}' contains the following extra files: {in_directory_b_but_not_a}"
        )
        for file in directory_a_files:
            assert filecmp.cmp(f"{directory_a}/{file}", f"{directory_b}/{file}"), (
                f"Difference detected for file '{file}' between the {directory_a} and {directory_b} directories"
            )
