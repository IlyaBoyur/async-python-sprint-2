from scheduler import Scheduler
from job import Job


class WebJob(Job):
    def run(self):
        import requests

        super().run()
        try:
            for url in [
                "https://google.com/",
                "https://ya.ru/",
                "https://www.rambler.ru/",
                "https://www.yahoo.com/",
            ]:
                response = requests.get(url)
                response.raise_for_status()
                yield response
        except RuntimeError:
            print("ERROR")


class IoJob(Job):
    def run(self):
        super().run()
        

if __name__ == "__main__":
    import time

    task1 = WebJob()
    task1 = IoJob()

    scheduler = Scheduler(3)
    scheduler.run()
    scheduler.schedule(task1)
    scheduler.schedule(task2)
    time.sleep(10)
    scheduler.stop()