from sqlalchemy import (
    Column, 
    Integer, 
    ForeignKey, 
    String,
    JSON, 
    Enum, 
    Boolean,
    DateTime, 
    UniqueConstraint,
    exists,
    and_,
    delete,
    select,
    func
)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.core import Base
from vvs_database.schemas.enums import JobStatus, JobType
from vvs_database.models.plugin_models import PluginExecutionFailure

class Job(Base):
    __tablename__ = "vvs_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(Enum(JobType), nullable=False)
    job_json = Column(MutableDict.as_mutable(JSON), nullable=True)
    # job_json = Column(JSON, nullable=True)
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.CREATED)
    status_detail = Column(MutableDict.as_mutable(JSON), nullable=True)
    # status_detail = Column(JSON, nullable=True)
    auto_execute = Column(Boolean, nullable=False)
    dagster_run_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    plugins = relationship("JobPlugin", back_populates="job", cascade="all, delete-orphan")
    execution_failures = relationship("PluginExecutionFailure", back_populates="job")

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'plugin',
        'polymorphic_load': 'selectin'
    }

    @classmethod
    async def cleanup_unreferenced(cls, session: AsyncSession):
        """
        Delete jobs that aren't referenced in vvs_job_plugins.
        Returns number of items deleted.
        """
        
        # workaround for circular import 
        from vvs_database.models.job_models.qdrant_upload import QdrantUploadFailed
        
        # Query for jobs that have no references
        unreferenced_jobs = select(cls.id).where(
            and_(
                ~exists().where(JobPlugin.job_id == cls.id),
                ~exists().where(PluginExecutionFailure.job_id == cls.id),
                ~exists().where(QdrantUploadFailed.job_id == cls.id)
            )
        )
        
        result = await session.execute(unreferenced_jobs)
        job_ids = result.scalars().all()
        
        # Use SQLAlchemy ORM to delete these objects properly
        deleted_count = 0
        for job_id in job_ids:
            job = await session.get(cls, job_id)
            if job:
                await session.delete(job)
                deleted_count += 1
        
        await session.commit()
        
        return deleted_count
    
class TestJob(Job):
    __tablename__ = "vvs_test_jobs"

    id = Column(Integer, ForeignKey("vvs_jobs.id"), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'test_job',
    }

class JobPlugin(Base):
    __tablename__ = "vvs_job_plugins"

    job_id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"), primary_key=True)
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), primary_key=True)
    
    job = relationship("Job", passive_deletes=True)
    plugin = relationship("Plugin", passive_deletes=True)
    
    __table_args__ = (
        UniqueConstraint('job_id', 'plugin_id', name='uix_job_plugin'),
    )
