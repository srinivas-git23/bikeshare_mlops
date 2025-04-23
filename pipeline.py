from typing import NamedTuple
from kfp.v2 import dsl, compiler
from kfp.v2.dsl import component, Output, Artifact
from google.cloud.aiplatform import pipeline_jobs




# -------------------------------
# COMPONENT: Validate Input Data
# -------------------------------
@component(
    base_image="python:3.11",
    packages_to_install=["gcsfs", "pandas", "google-cloud-storage"]
)
def validate_input_data(filename: str) -> NamedTuple("output", [("validation", str)]):
    import logging
    import pandas as pd
    from google.cloud import storage

    logging.basicConfig(level=logging.INFO)
    logging.info(f"Reading file: {filename}")
    df = pd.read_csv(filename)
    
    validation = "true"
    expected_cols = [
        'instant', 'dteday', 'season', 'yr', 'mnth', 'hr', 'holiday',
        'weekday', 'workingday', 'weathersit', 'temp', 'atemp',
        'hum', 'windspeed', 'casual', 'registered', 'cnt'
    ]
    
    if len(df.columns) != len(expected_cols) or set(df.columns) != set(expected_cols):
        validation = "false"

    return (validation,)


# -------------------------------
# COMPONENT: Custom Training Job
# -------------------------------
@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-aiplatform", "gcsfs", "scikit-learn", "pandas", "google-cloud-storage"]
)
def custom_training_job_component(script_url: str):
    import os
    import requests
    import logging
    from google.cloud import aiplatform

    logging.basicConfig(level=logging.INFO)
    aiplatform.init(
        project="canvas-joy-456715-b1",
        location="us-east1",
        staging_bucket="gs://bikeshare-7131"
    )

    # Download training script from GitHub
    response = requests.get(script_url)
    with open("model-training-code.py", "wb") as f:
        f.write(response.content)

    job = aiplatform.CustomTrainingJob(
        display_name="bikeshare-training-job",
        script_path="model-training-code.py",
        container_uri="us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.0-23:latest",
        requirements=["gcsfs"]
    )

    job.run(replica_count=1, machine_type="n1-standard-4", sync=True)


# -------------------------------
# COMPONENT: Model Deployment

# -------------------------------
@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-aiplatform"]
)
def deploy_model_component() -> NamedTuple("endpoint", [("endpoint", str)]):
    from google.cloud import aiplatform

    aiplatform.init(
        project="canvas-joy-456715-b1",
        location="us-east1",
        staging_bucket="gs://bikeshare-7131"
    )

    model = aiplatform.Model.upload(
        display_name="bikeshare-model",
        artifact_uri="gs://bikeshare-7131/artifact/",
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
        sync=False
    )

    endpoint = model.deploy(
        deployed_model_display_name="bikeshare-endpoint",
        traffic_split={"0": 100},
        machine_type="n2-standard-4",
        min_replica_count=1,
        max_replica_count=1
    )
    
    
    
    

# -------------------------------
# PIPELINE: Validation → Training → Deployment
# -------------------------------
@dsl.pipeline(
    pipeline_root="gs://bikeshare-7131/bikeshare-pipeline-root",
    name="bikeshare-full-pipeline"
)
def bikeshare_pipeline(
    project: str = "canvas-joy-456715-b1",
    region: str = "us-east1",
    filename: str = "gs://bikeshare-7131/hour.csv",
    script_url: str = "https://raw.githubusercontent.com/srinivas-git23/bikeshare_mlops/main/model-training-code.py"
):
    validate_op = validate_input_data(filename=filename)

    with dsl.Condition(validate_op.outputs["validation"] == "true", name="CheckValidDataset"):
        training_op = custom_training_job_component(script_url=script_url).after(validate_op)
        deploy_model_component().after(training_op)


# -------------------------------
# COMPILE + TRIGGER PIPELINE
# -------------------------------
if __name__ == "__main__":

    
    
    compiler.Compiler().compile(pipeline_func=bikeshare_pipeline, package_path=bikeshare_full_pipeline.json)

    pipeline_job = pipeline_jobs.PipelineJob(
        display_name="bikeshare-full-pipeline-run",
        template_path=bikeshare_full_pipeline.json,
        enable_caching=False,
        location="us-east1",
        project="canvas-joy-456715-b1"
    )
    pipeline_job.run()



    
