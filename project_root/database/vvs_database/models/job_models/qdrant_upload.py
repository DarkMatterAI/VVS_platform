from sqlalchemy import (
    Column, 
    Integer, 
    Float,
    ForeignKey, 
    String,
    Boolean,
)
from sqlalchemy.orm import relationship

from vvs_database.core import Base
from vvs_database.models.job_models.job_models import Job

class QdrantUploadJob(Job):
    __tablename__ = "qdrant_upload"

    # id = Column(Integer, ForeignKey("vvs_jobs.id"), primary_key=True)
    id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"), primary_key=True)
    num_uploaded = Column(Integer, nullable=True)
    num_failed = Column(Integer, nullable=True)
    index_time = Column(Float, nullable=True)
    index_timeout = Column(Boolean, nullable=True)
    index_error = Column(Boolean, nullable=True)

    __mapper_args__ = {
        'polymorphic_identity': 'qdrant_upload',
    }

class QdrantUploadFailed(Base):
    __tablename__ = "qdrant_upload_failed"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("qdrant_upload.id", ondelete="CASCADE"))
    item = Column(String, nullable=False)
    external_id = Column(String, nullable=True)

    job = relationship("QdrantUploadJob", passive_deletes=True)
