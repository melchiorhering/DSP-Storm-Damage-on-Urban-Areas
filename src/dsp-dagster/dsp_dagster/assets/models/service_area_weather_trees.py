import os
from dagster import job, op, get_dagster_logger, asset



@asset(name="my_op")
def get_file_sizes():
    files = [f for f in os.listdir(".") if os.path.isfile(f)]
    for f in files:
        get_dagster_logger().info(f"Size of {f} is {os.path.getsize(f)}")

# from dagster import op, job
# from dagster_mlflow import end_mlflow_on_run_finished, mlflow_tracking

# @op(required_resource_keys={"mlflow"})
# def mlflow_op(context):
#     mlflow.log_params({"param": "param"})
#     mlflow.tracking.MlflowClient().create_registered_model("SOME MODEL")

# @end_mlflow_on_run_finished
# @job(resource_defs={"mlflow": mlflow_tracking})
# def mlf_example():
#     mlflow_op()

# # example using an mlflow instance with s3 storage
# mlf_example.execute_in_process(run_config={
#     "resources": {
#         "mlflow": {
#             "config": {
#                 "experiment_name": "TEST",
#                 "mlflow_tracking_uri": "http://localhost:5001",

#                 # # if want to run a nested run, provide parent_run_id
#                 # "parent_run_id": an_existing_mlflow_run_id,

#                 # # env variables to pass to mlflow
#                 # "env": {
#                 #     "MLFLOW_S3_ENDPOINT_URL": my_s3_endpoint,
#                 #     "AWS_ACCESS_KEY_ID": my_aws_key_id,
#                 #     "AWS_SECRET_ACCESS_KEY": my_secret,
#                 # },

#                 # env variables you want to log as mlflow tags
#                 "env_to_tag": ["DOCKER_IMAGE_TAG"],

#                 # key-value tags to add to your experiment
#                 "extra_tags": {"super": "experiment"},
#             }
#         }
#     }
# })