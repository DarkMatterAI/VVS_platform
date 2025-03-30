from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from vvs_database import crud, logging, schemas, utils  

class JobRunner():
    def __init__(self, job_id):
        self.job_id = job_id
        self.job = None 
        self.plugins = {} 
        self.log_id = None 

    async def load_job(self, db_session: AsyncSession):
        logging.info(f"Loading job {self.job_id}")
        job = await crud.get_job(db_session, self.job_id, load_plugins=True)
        self.job = job 
        
        for jp in job.plugins:
            plugin = jp.plugin
            self.plugins[plugin.id] = utils.get_plugin_response_model(plugin)

        self.log_id = f"Job {self.job.id}"

    async def update_job(self, 
                         db_session: AsyncSession, 
                         status: schemas.JobStatus, 
                         status_detail: Optional[dict]=None,
                         dagster_run_id: Optional[str]=None):
        logging.info(f"Updating job {self.job_id}")
        self.job = await crud.update_job(db_session, 
                                         self.job_id, 
                                         status=status, 
                                         status_detail=status_detail,
                                         dagster_run_id=dagster_run_id)


