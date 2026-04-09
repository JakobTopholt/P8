import os
import platform
import shutil
from pathlib import Path
from typing import Optional


HADOOP_EXECUTABLE = "hadoop.cmd" if os.name == "nt" else "hadoop"


def _prepend_env_path(env_var: str, path: Path) -> None:
    path_str = str(path)
    current_path = os.environ.get(env_var, "")
    path_entries = current_path.split(os.pathsep) if current_path else []
    if path_str not in path_entries:
        os.environ[env_var] = path_str + (os.pathsep + current_path if current_path else "")


def _prepend_path(path: Path) -> None:
    _prepend_env_path("PATH", path)


def _is_hadoop_home(candidate: Path) -> bool:
    if not candidate.is_dir():
        return False

    if (candidate / "bin" / HADOOP_EXECUTABLE).is_file():
        return True

    # Allow detecting installs where the executable is named "hadoop".
    return (candidate / "bin" / "hadoop").is_file()


def _hadoop_home_from_path_command(hadoop_cmd: str) -> Optional[Path]:
    hadoop_path = Path(hadoop_cmd).resolve()
    candidate = hadoop_path.parent.parent
    if _is_hadoop_home(candidate):
        return candidate.resolve()
    return None


def _iter_dir_children(parent: Path) -> list[Path]:
    if not parent.is_dir():
        return []

    try:
        return sorted(
            [entry for entry in parent.iterdir() if entry.is_dir()],
            reverse=True,
        )
    except OSError:
        return []


def _system_hadoop_home() -> Optional[Path]:
    env_hadoop_home = os.environ.get("HADOOP_HOME")
    if env_hadoop_home:
        candidate = Path(env_hadoop_home).expanduser()
        if _is_hadoop_home(candidate):
            return candidate.resolve()

    hadoop_cmd = shutil.which("hadoop")
    if hadoop_cmd:
        candidate = _hadoop_home_from_path_command(hadoop_cmd)
        if candidate:
            return candidate

    system_name = platform.system()
    candidate_homes: list[Path] = []

    if system_name == "Windows":
        candidate_homes.extend(
            [
                Path(r"C:\hadoop"),
                Path(r"C:\Program Files\Hadoop"),
                Path(r"C:\Program Files\hadoop"),
                Path(r"C:\opt\hadoop"),
            ]
        )
    elif system_name == "Darwin":
        candidate_homes.extend(
            [
                Path("/opt/homebrew/opt/hadoop/libexec"),
                Path("/usr/local/opt/hadoop/libexec"),
                Path("/usr/local/hadoop"),
                Path("/opt/hadoop"),
            ]
        )
        candidate_homes.extend(_iter_dir_children(Path("/opt/homebrew/Cellar/hadoop")))
        candidate_homes.extend(_iter_dir_children(Path("/usr/local/Cellar/hadoop")))
    else:
        candidate_homes.extend(
            [
                Path("/usr/local/hadoop"),
                Path("/opt/hadoop"),
                Path("/usr/lib/hadoop"),
                Path("/usr/lib/hadoop-current"),
            ]
        )

    for candidate in candidate_homes:
        if _is_hadoop_home(candidate):
            return candidate.resolve()

    return None


def _local_hadoop_home(project_dir: Path) -> Optional[Path]:
    local_root = project_dir / "local_hadoop"
    candidate_homes = [local_root / "current", local_root / "hadoop"]
    candidate_homes.extend(_iter_dir_children(local_root))

    for candidate in candidate_homes:
        if _is_hadoop_home(candidate):
            return candidate.resolve()

    # Legacy fallback for old project layout.
    legacy_home = project_dir / "hadoop"
    if _is_hadoop_home(legacy_home):
        return legacy_home.resolve()

    return None


def _configure_native_library_paths(hadoop_home: Path) -> None:
    native_dir = hadoop_home / "lib" / "native"
    if not native_dir.is_dir():
        return

    os.environ["HADOOP_COMMON_LIB_NATIVE_DIR"] = str(native_dir)

    java_library_opt = f"-Djava.library.path={native_dir}"
    spark_submit_opts = os.environ.get("SPARK_SUBMIT_OPTS", "")
    if java_library_opt not in spark_submit_opts:
        os.environ["SPARK_SUBMIT_OPTS"] = (
            f"{java_library_opt} {spark_submit_opts}".strip()
        )

    system_name = platform.system()
    if system_name == "Linux":
        _prepend_env_path("LD_LIBRARY_PATH", native_dir)
    elif system_name == "Darwin":
        _prepend_env_path("DYLD_LIBRARY_PATH", native_dir)


def _activate_hadoop_home(hadoop_home: Path, verbose: bool, source: str) -> None:
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    _prepend_path(hadoop_home / "bin")
    _configure_native_library_paths(hadoop_home)

    if verbose:
        print(f"Using {source} Hadoop home: {hadoop_home}")


def configure_hadoop_environment(project_dir: Path, verbose: bool = True) -> None:
    system_hadoop_home = _system_hadoop_home()
    if system_hadoop_home:
        _activate_hadoop_home(system_hadoop_home, verbose=verbose, source="system")
        return

    local_hadoop_home = _local_hadoop_home(project_dir)
    if local_hadoop_home:
        _activate_hadoop_home(local_hadoop_home, verbose=verbose, source="project-local")
        return

    if verbose:
        print(
            "Warning: Hadoop environment was not found. Install Hadoop system-wide, "
            "set HADOOP_HOME, or add a portable build under "
            f"'{project_dir / 'local_hadoop'}'."
        )
