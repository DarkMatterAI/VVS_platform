from dagster import Definitions, load_assets_from_modules

from vvs_dagster import assets, resources, test_job, qdrant_upload_job  # noqa: TID252

all_assets = load_assets_from_modules([assets, qdrant_upload_job])

defs = Definitions(
    assets=all_assets,
    resources=resources.RESOURCE_DEFAULTS,
    jobs=[test_job.get_plugin_job,
          qdrant_upload_job.qdrant_upload_job]
)
