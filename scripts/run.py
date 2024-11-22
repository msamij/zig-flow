import subprocess
from pathlib import Path

PROJECT_ROOT = Path.cwd()
SPARK_PIPELINE = PROJECT_ROOT.joinpath('sparkpipeline')
TARGET_FOLDER = PROJECT_ROOT.joinpath(SPARK_PIPELINE).joinpath('target')

jar_file: str


def find_jar_file() -> str:
    for file in TARGET_FOLDER.iterdir():
        if (str(file).endswith('.jar')):
            return file
    raise FileNotFoundError(
        f'No jar file found in {TARGET_FOLDER} folder. Run mvn commands to first build the jar file.')


def run_maven() -> None:
    """Run Maven validate install and package command"""
    command = 'mvn validate && mvn clean && mvn install && mvn package'
    try:
        print('***Running mvn commands***')
        subprocess.run(command, cwd=SPARK_PIPELINE, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f'mvn command failed: {e}')


def run_spark_submit(jar_file: str) -> None:
    class_file = 'com.msamiaj.zigflow.Main'
    command = f'spark-submit --class {class_file} {jar_file}'
    try:
        print('***Running spark***')
        subprocess.run(command, cwd=SPARK_PIPELINE, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f'spark execution failed: {e}')


if __name__ == '__main__':
    try:
        jar_file = find_jar_file()
        run_maven()
        run_spark_submit(jar_file)
    except Exception as e:
        print(f'Pipeline execution failed! {e}')
