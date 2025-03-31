from sqlalchemy import (
    Column, 
    Integer, 
    Float,
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
    func
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.core import Base
from vvs_database.schemas.enums import JobStatus, JobType
from vvs_database.models.plugin_models import PluginExecutionFailure

class Job(Base):
    __tablename__ = "vvs_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(Enum(JobType), nullable=False)
    job_json = Column(JSON, nullable=True)
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.CREATED)
    status_detail = Column(JSON, nullable=True)
    auto_execute = Column(Boolean, nullable=False)
    dagster_run_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
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
        delete_stmt = delete(cls).where(
            and_(
                ~exists().where(JobPlugin.job_id == cls.id),
                ~exists().where(PluginExecutionFailure.job_id == cls.id),
                ~exists().where(QdrantUploadFailed.job_id == cls.id)
            )
        ).returning(cls.id)

        result = await session.execute(delete_stmt)
        deleted_rows = result.scalars().all()
        
        await session.commit()
        
        return len(deleted_rows)
    
class TestJob(Job):
    __tablename__ = "vvs_test_jobs"

    id = Column(Integer, ForeignKey("vvs_jobs.id"), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'test_job',
    }

class QdrantUploadJob(Job):
    __tablename__ = "qdrant_upload"

    id = Column(Integer, ForeignKey("vvs_jobs.id"), primary_key=True)
    num_uploaded = Column(Integer, nullable=True)
    num_failed = Column(Integer, nullable=True)
    index_time = Column(Float, nullable=True)
    index_timeout = Column(Boolean, nullable=True)
    index_error = Column(Boolean, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': 'qdrant_upload',
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

class QdrantUploadFailed(Base):
    __tablename__ = "qdrant_upload_failed"

    id = Column(Integer, primary_key=True, index=True)
    # job_id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"))
    job_id = Column(Integer, ForeignKey("qdrant_upload.id", ondelete="CASCADE"))
    item = Column(String, nullable=False)
    external_id = Column(String, nullable=True)

    # job = relationship("Job", passive_deletes=True)
    job = relationship("QdrantUploadJob", passive_deletes=True)

