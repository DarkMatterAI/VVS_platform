from dagster import Definitions, load_assets_from_modules

from vvs_dagster import assets, resources, dynamic_job, qdrant_upload_job

all_assets = load_assets_from_modules([assets, qdrant_upload_job])

defs = Definitions(
    assets=all_assets,
    resources=resources.RESOURCE_DEFAULTS,
    jobs=[dynamic_job.dynamic_job,
          qdrant_upload_job.qdrant_upload_job],
    sensors=[qdrant_upload_job.qdrant_upload_sensor]
)
