from openhexa.sdk import current_run, pipeline


@pipeline("update-excel", name="Update Excel")
def update_excel():
    count = task_1()
    task_2(count)


@update_excel.task
def task_1():
    current_run.log_info("In task 1...")

    return 42


@update_excel.task
def task_2(count):
    current_run.log_info(f"In task 2... count is {count}")


if __name__ == "__main__":
    update_excel()
