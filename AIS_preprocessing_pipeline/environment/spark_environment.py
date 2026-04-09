import os
import platform
import sys
from pathlib import Path


def _spark_python_executable() -> str:
    python_exec = sys.executable

    if platform.system() == "Windows":
        import ctypes

        # Spark workers on Windows can fail when paths contain spaces.
        buf = ctypes.create_unicode_buffer(260)
        if ctypes.windll.kernel32.GetShortPathNameW(sys.executable, buf, 260):
            python_exec = buf.value

    return python_exec


def configure_pyspark_python() -> None:
    python_exec = _spark_python_executable()
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec


def configure_spark_environment(project_dir: Path) -> None:
    spark_temp_dir = project_dir / "spark_temp"
    spark_temp_dir.mkdir(parents=True, exist_ok=True)
    spark_conf_dir = project_dir / "ais_pipeline" / "environment" / "spark_conf"

    # Allow user-defined values to win, but avoid common local startup warnings.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_DIRS", str(spark_temp_dir))
    if (spark_conf_dir / "log4j2.properties").is_file():
        os.environ.setdefault("SPARK_CONF_DIR", str(spark_conf_dir))
