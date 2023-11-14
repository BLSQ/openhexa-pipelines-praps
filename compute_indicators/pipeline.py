import os

import papermill as pm
from openhexa.sdk import current_run, pipeline, workspace, parameter


@pipeline("compute-indicators", name="Calculer les indicateurs")
@parameter(
    "update",
    name="Update dashboards",
    help="Push extracted surveys to the dashboard database",
    type=bool,
    default=False,
)
def compute_indicators(update: bool):
    run_notebook(update)


@compute_indicators.task
def run_notebook(update: bool):
    current_run.log_info("Computing indicators...")
    pm.execute_notebook(
        input_path=os.path.join(
            workspace.files_path,
            "pipelines",
            "extract-transform-load",
            "compute-indicators.ipynb",
        ),
        output_path=os.path.join(
            workspace.files_path,
            "pipelines",
            "extract-transform-load",
            "data",
            "output",
            "notebooks",
            "output.ipynb",
        ),
        parameters={"update_dashboard_database": update},
        request_save_on_cell_execute=False,
        progress_bar=False,
    )
    current_run.log_info("Finished computing indicators")

    output_dir = os.path.join(
        workspace.files_path, "pipelines", "extract-transform-load", "data", "output"
    )
    for fname in os.listdir(output_dir):
        if (
            fname.startswith("IRI-")
            or fname.startswith("IR-")
            or fname.startswith("Reg")
        ) and fname.endswith(".csv"):
            current_run.add_file_output(os.path.join(output_dir, fname))


if __name__ == "__main__":
    compute_indicators()
