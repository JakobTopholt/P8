import os
import platform
from pathlib import Path


def _prepend_path(path: Path) -> None:
    path_str = str(path)
    current_path = os.environ.get("PATH", "")
    path_entries = current_path.split(os.pathsep) if current_path else []
    if path_str not in path_entries:
        os.environ["PATH"] = path_str + (os.pathsep + current_path if current_path else "")


def configure_hadoop_environment(project_dir: Path) -> None:
    if platform.system() != "Windows":
        return

    candidate_homes = []

    env_hadoop_home = os.environ.get("HADOOP_HOME")
    if env_hadoop_home:
        candidate_homes.append(Path(env_hadoop_home))

    candidate_homes.extend([Path(r"C:\hadoop"), project_dir / "hadoop"])

    for candidate in candidate_homes:
        if candidate.is_dir():
            os.environ["HADOOP_HOME"] = str(candidate)
            _prepend_path(candidate / "bin")
            print(f"Using Hadoop home: {candidate}")
            return

    print(
        "Warning: Hadoop environment was not found. Set HADOOP_HOME, install Hadoop at "
        r"'C:\hadoop', or provide a project-local 'hadoop' directory."
    )
