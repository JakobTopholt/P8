import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Optional

JAVA_EXECUTABLE = "java.exe" if os.name == "nt" else "java"


def _prepend_path(path: Path) -> None:
    path_str = str(path)
    current_path = os.environ.get("PATH", "")
    path_entries = current_path.split(os.pathsep) if current_path else []
    if path_str not in path_entries:
        os.environ["PATH"] = path_str + (os.pathsep + current_path if current_path else "")


def _java_command_is_usable(java_cmd: str) -> bool:
    try:
        completed = subprocess.run(
            [java_cmd, "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


def _is_java_home(candidate: Path) -> bool:
    return (candidate / "bin" / JAVA_EXECUTABLE).is_file()


def _java_home_from_path_command(java_cmd: str) -> Optional[Path]:
    java_path = Path(java_cmd).resolve()
    candidate = java_path.parent.parent
    if _is_java_home(candidate):
        return candidate.resolve()
    return None


def _system_java_home() -> Optional[Path]:
    env_java_home = os.environ.get("JAVA_HOME")
    if env_java_home:
        candidate = Path(env_java_home)
        if _is_java_home(candidate):
            java_cmd = str(candidate / "bin" / JAVA_EXECUTABLE)
            if _java_command_is_usable(java_cmd):
                return candidate.resolve()

    java_cmd = shutil.which("java")
    if not java_cmd or not _java_command_is_usable(java_cmd):
        return None

    return _java_home_from_path_command(java_cmd)


def _linux_portable_java_home(project_dir: Path) -> Optional[Path]:
    if platform.system() != "Linux":
        return None

    local_java_root = project_dir / "local_java"
    candidates = [local_java_root]
    first_level = []

    if local_java_root.is_dir():
        try:
            first_level = [entry for entry in local_java_root.iterdir() if entry.is_dir()]
        except OSError:
            pass

    candidates.extend(first_level)

    for subdir in first_level:
        try:
            candidates.extend(entry for entry in subdir.iterdir() if entry.is_dir())
        except OSError:
            continue

    for candidate in candidates:
        if _is_java_home(candidate):
            java_cmd = str(candidate / "bin" / JAVA_EXECUTABLE)
            if _java_command_is_usable(java_cmd):
                return candidate.resolve()

    return None


def configure_java_environment(project_dir: Path) -> None:
    system_java_home = _system_java_home()
    if system_java_home:
        os.environ["JAVA_HOME"] = str(system_java_home)
        _prepend_path(system_java_home / "bin")
        print(f"Using system Java: {system_java_home}")
        return

    java_cmd = shutil.which("java")
    if java_cmd and _java_command_is_usable(java_cmd):
        print(f"Using system Java from PATH: {java_cmd}")
        return

    portable_java_home = _linux_portable_java_home(project_dir)
    if portable_java_home:
        os.environ["JAVA_HOME"] = str(portable_java_home)
        _prepend_path(portable_java_home / "bin")
        print(f"Using portable Linux Java: {portable_java_home}")
        return

    raise RuntimeError(
        "Java environment could not be found. Install Java/JDK on your system, "
        "or if you are on Linux, add a portable Java runtime under "
        f"'{project_dir / 'local_java'}'."
    )
