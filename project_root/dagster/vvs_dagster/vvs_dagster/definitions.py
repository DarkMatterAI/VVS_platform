from dagster import Definitions, load_assets_from_modules

from vvs_dagster import assets, resources, test_job  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources=resources.RESOURCE_DEFAULTS,
    jobs=[test_job.get_plugin_job]
)
