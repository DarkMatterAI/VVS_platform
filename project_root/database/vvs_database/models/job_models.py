from sqlalchemy import Column, Integer, String, ForeignKey, JSON, Enum, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from vvs_database.core import Base
from vvs_database.schemas.enums import JobStatus, JobType

class Job(Base):
    __tablename__ = "vvs_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(Enum(JobType), nullable=False)
    job_json = Column(JSON, nullable=True)
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.CREATED)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationship to JobPlugins
    plugins = relationship("JobPlugin", back_populates="job", cascade="all, delete-orphan")

class JobPlugin(Base):
    __tablename__ = "vvs_job_plugins"

    job_id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"), primary_key=True)
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), primary_key=True)
    
    job = relationship("Job", passive_deletes=True)
    plugin = relationship("Plugin", passive_deletes=True)
    
    __table_args__ = (
        UniqueConstraint('job_id', 'plugin_id', name='uix_job_plugin'),
    )