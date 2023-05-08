from openhexa.sdk import current_run, pipeline, parameter


@pipeline("compute-indicators", name="Compute Indicators")
@parameter(
    "include_kobo",
    name="Include KoboToolbox data",
    help="Include data from KoboToolbox surveys",
    type=bool,
    default=True,
)
@parameter(
    "include_excel",
    name="Include Excel data",
    help="Include survey data from Excel sheets",
    type=bool,
    default=True,
)
@parameter(
    "include_praps1",
    name="Include PRAPS 1 data",
    help="Include data from PRAPS 1",
    type=bool,
    default=False,
)
@parameter(
    "postgres_table",
    name="Database table",
    help="Target table in the workspace database",
    type=str,
    required=False,
)
def compute_indicators(include_kobo, include_excel, include_praps1, postgres_table):
    current_run.log_info("Consolidating source data...")
    current_run.log_info("Computing indicators...")


if __name__ == "__main__":
    compute_indicators()
