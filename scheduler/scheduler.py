import sys
import os
import time
import schedule

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def schedule_spark_pipeline() -> None:
    jar_file = find_jar_file()
    run_spark_submit(jar_file)


def main() -> None:
    schedule_spark_pipeline()
    schedule.every(20).minutes.do(schedule_spark_pipeline)

    print("Scheduler is running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    from scripts.run import *
    main()
