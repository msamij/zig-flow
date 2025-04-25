import subprocess
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

SPARK_MASTER = 'local[3]'
DRIVER_MEMORY = '6g'

PROJECT_ROOT = Path.cwd()
SPARK_PIPELINE = PROJECT_ROOT.joinpath('sparkpipeline')
TARGET_FOLDER = PROJECT_ROOT.joinpath(SPARK_PIPELINE).joinpath('target')


def run_maven() -> None:
    """Executes the basic maven commands clean, install and package command."""
    command = 'mvn clean && mvn install'
    try:
        logger.info('***Running mvn commands (mvn clean && mvn install)***')
        subprocess.run(command, cwd=SPARK_PIPELINE, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f'mvn command failed: {e}')


def find_jar_file() -> str:
    for file in TARGET_FOLDER.iterdir():
        if (str(file).endswith('.jar')):
            return file
    raise FileNotFoundError(
        f'No jar file found in {TARGET_FOLDER} folder. Run mvn commands to first build the jar file.')


def run_spark_submit(jar_file: str) -> None:
    class_file = 'com.msamiaj.zigflow.Main'
    command = f'spark-submit --master {SPARK_MASTER} --driver-memory {DRIVER_MEMORY} --class {class_file} {jar_file}'
    try:
        logger.info(f'***Running spark job on master {SPARK_MASTER}***')
        subprocess.run(command, cwd=SPARK_PIPELINE, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f'spark execution failed: {e}')


if __name__ == '__main__':
    try:
        run_maven()
        jar_file = find_jar_file()
        run_spark_submit(jar_file)
    except Exception as e:
        logger.error(f'Pipeline execution failed! {e}')
