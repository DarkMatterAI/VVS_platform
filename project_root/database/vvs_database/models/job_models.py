from sqlalchemy import (
    Column, 
    Integer, 
    ForeignKey, 
    String,
    JSON, 
    Enum, 
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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    plugins = relationship("JobPlugin", back_populates="job", cascade="all, delete-orphan")
    execution_failures = relationship("PluginExecutionFailure", back_populates="job")

    @classmethod
    async def cleanup_unreferenced(cls, session: AsyncSession):
        """
        Delete jobs that aren't referenced in vvs_job_plugins.
        Returns number of items deleted.
        """
        delete_stmt = delete(cls).where(
            and_(
                ~exists().where(JobPlugin.job_id == cls.id),
                ~exists().where(PluginExecutionFailure.job_id == cls.id)
            )
        ).returning(cls.id)

        result = await session.execute(delete_stmt)
        deleted_rows = result.scalars().all()
        
        await session.commit()
        
        return len(deleted_rows)

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
    job_id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"))
    item = Column(String, nullable=False)
    external_id = Column(String, nullable=True)

    job = relationship("Job", passive_deletes=True)

