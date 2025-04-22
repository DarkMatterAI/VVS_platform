from dagster import Definitions, load_assets_from_modules

from vvs_dagster import (
    assets, 
    resources, 
    qdrant_upload_job, 
    test_job, 
    failure_sensor,
    hc_job
)

all_assets = load_assets_from_modules([assets, qdrant_upload_job])

defs = Definitions(
    assets=all_assets,
    resources=resources.RESOURCE_DEFAULTS,
    jobs=[test_job.test_job,
          qdrant_upload_job.qdrant_upload_job,
          hc_job.hc_job],
    sensors=[qdrant_upload_job.qdrant_upload_sensor,
             failure_sensor.job_canceled_sensor,
             failure_sensor.job_failure_sensor]
)

