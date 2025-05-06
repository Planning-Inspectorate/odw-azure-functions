import subprocess
from typing import List


class Util:
    """
        Class that has useful utility functions
    """
    @classmethod
    def run_az_cli_command(cls, args: List[str]):
        """
            Run an az cli command. Raises a `RuntimeException` if something goes wrong
        """
        print(f"Running command: '{' '.join(args)}'")
        try:
            return subprocess.check_output(args)
        except subprocess.CalledProcessError as e:
            exception = e
        raise RuntimeError(f"Exception raised when running the above command: {exception.output.decode('utf-8')}")
