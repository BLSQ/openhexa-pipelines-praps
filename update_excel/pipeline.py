from openhexa.sdk import current_run, pipeline, parameter


@pipeline("update-excel", name="Update Excel")
@parameter(
    "data_directory",
    name="Data directory",
    help="Source directory with Excel sheets",
    type=str,
    required=True,
)
def update_excel():
    current_run.log_info("Extracting survey data from Excel sheets")


if __name__ == "__main__":
    update_excel()
