import dagster as dg
from dagster_aws.s3 import S3Resource

@dg.asset
def list_s3_assets(context: dg.AssetExecutionContext,
                   s3_resource: S3Resource):
    s3_client = s3_resource.get_client()
    objects = s3_client.list_objects(Bucket='vvs-bucket')
    objects = [i for i in objects['Contents']]
    context.log.info(f"Objects: {objects}")

