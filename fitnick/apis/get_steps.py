from time import sleep

from tqdm import tqdm

from fitnick.activity.activity import Activity
from datetime import datetime

from win10toast import ToastNotifier

toaster = ToastNotifier()


def get_steps_for_day():
    steps_last_time = 0
    steps_this_time = 0
    for _ in range(1, 10):
        activity_api = Activity(config={'base_date': '2020-12-13'})
        response = activity_api.query_daily_activity_summary()

        steps_this_time = response['summary']['steps']
        dt = datetime.now()

        with open('steps.csv', 'a') as f:
            f.write(f"{steps_this_time},{dt}\n")

        # Show notification whenever needed
        toaster.show_toast(
            "Fitnick Alert",
            f"You have taken {steps_this_time} steps so far today! Keep it up!",
            threaded=False,
            icon_path=None,
            duration=3  # 3 seconds
        )

        print(str(steps_this_time), str(steps_last_time))

        toaster.show_toast(
            "Fitnick Alert",
            f"You took {steps_this_time - steps_last_time} steps since my last alert!",
            threaded=False,
            icon_path=None,
            duration=5
        )

        steps_last_time = steps_this_time
        for _ in tqdm(range(0, 600)):
            sleep(1)

    return int(steps_this_time)


def main():
    steps = get_steps_for_day()
    return steps


if __name__ == '__main__':
    main()
