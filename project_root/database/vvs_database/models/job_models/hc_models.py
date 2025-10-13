from sqlalchemy import (
    Column, 
    Integer, 
    ForeignKey, 
    UniqueConstraint,
    JSON, 
    Boolean,
    String,
    DateTime,
    func,
    and_,
    select,
    Index
)
from sqlalchemy.ext.mutable import MutableDict

from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.core import Base
from vvs_database.models.job_models.job_models import Job

class HCJob(Job):
    __tablename__ = "hc_jobs"

    id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"), primary_key=True)
    num_inputs = Column(Integer, nullable=True)
    inference_limit = Column(Integer, nullable=True)
    time_limit = Column(Integer, nullable=True)
    inference = Column(Integer, nullable=True)

    inputs = relationship("HCInputJob", 
                         back_populates="parent", 
                         cascade="all, delete-orphan",
                         foreign_keys="[HCInputJob.parent_id]")
    results = relationship("HCResult", back_populates="job", cascade="all, delete-orphan")

    __mapper_args__ = {
        'polymorphic_identity': 'hill_climb_job',
    }

class HCInputJob(Job):
    __tablename__ = "hc_input_jobs"

    id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"), primary_key=True)
    parent_id = Column(Integer, ForeignKey("hc_jobs.id", ondelete="CASCADE"), nullable=False)
    max_iterations = Column(Integer, nullable=False)
    inference_limit = Column(Integer, nullable=True)
    time_limit = Column(Integer, nullable=True)
    inference = Column(Integer, nullable=True)

    parent = relationship("HCJob", 
                         back_populates="inputs", 
                         passive_deletes=True,
                         foreign_keys=[parent_id])
    input_items = relationship("HCInputItems", 
                             back_populates="input_job",
                             cascade="all, delete-orphan")
    iterations = relationship("HCIterationJob", 
                             back_populates="input_job", 
                             cascade="all, delete-orphan",
                             primaryjoin="HCInputJob.id==HCIterationJob.input_id")

    __mapper_args__ = {
        'polymorphic_identity': 'hill_climb_job_input',
    }

    __table_args__ = (
        Index('idx_hc_input_jobs_parent', 'parent_id'),
    )

class HCInputItems(Base):
    __tablename__ = "hc_input_items"
    
    job_id = Column(Integer, ForeignKey("hc_input_jobs.id", ondelete="CASCADE"), primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), primary_key=True)
    assembly_index = Column(Integer, primary_key=True)
    external_id = Column(String, nullable=True)
    
    input_job = relationship("HCInputJob", back_populates="input_items")
    item = relationship("Item")

    __table_args__ = (
        Index('idx_hc_input_items_job', 'job_id'),
        Index('idx_hc_input_items_item', 'item_id'),
    )

class HCIterationJob(Job):
    __tablename__ = "hc_iteration_jobs"

    id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"), primary_key=True)
    input_id = Column(Integer, ForeignKey("hc_input_jobs.id", ondelete="CASCADE"), nullable=False)
    parent_id = Column(Integer, ForeignKey("hc_iteration_jobs.id"), nullable=True)
    iteration = Column(Integer, nullable=False)
    inference = Column(Integer, nullable=True)
    query_embedding = Column(MutableDict.as_mutable(JSON), nullable=True)

    input_job = relationship("HCInputJob", 
                            back_populates="iterations", 
                            passive_deletes=True,
                            foreign_keys=[input_id])
                            
    parent = relationship("HCIterationJob", 
                         remote_side=[id], 
                         backref=backref("children"),
                         foreign_keys=[parent_id])
                         
    iteration_results = relationship("HCIterationResult", 
                                    back_populates="iteration_job", 
                                    cascade="all, delete-orphan")

    __mapper_args__ = {
        'polymorphic_identity': 'hill_climb_job_iteration',
    }

    __table_args__ = (
        UniqueConstraint('input_id', 'iteration', name='uix_input_id_iteration'),
        Index('idx_hc_iteration_jobs_input',  'input_id'),
        Index('idx_hc_iteration_jobs_parent', 'parent_id'),
    )

class HCResult(Base):
    __tablename__ = "hc_results"

    result_id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("hc_jobs.id", ondelete="CASCADE"), nullable=False)
    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), nullable=False)
    assembly_id = Column(Integer, ForeignKey("assemblies.assembly_id", ondelete="CASCADE"), nullable=True)
    valid = Column(Boolean, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    job = relationship("HCJob", back_populates="results", passive_deletes=True)
    item = relationship("Item", passive_deletes=True)
    assembly = relationship("Assembly", passive_deletes=True)
    iteration_results = relationship("HCIterationResult", 
                                    back_populates="result", 
                                    cascade="all, delete-orphan")

    __table_args__ = (
        # (A) rows whose assembly_id IS NOT NULL
        Index(
            "uix_hc_result_notnull",
            "job_id", "item_id", "assembly_id",
            unique=True,
            postgresql_where=assembly_id.isnot(None),
        ),
        # (B) rows whose assembly_id IS NULL
        Index(
            "uix_hc_result_null",
            "job_id", "item_id",
            unique=True,
            postgresql_where=assembly_id.is_(None),
        ),
        Index('idx_hc_results_job', 'job_id'),
        Index('idx_hc_results_item', 'item_id'),
    )

class HCIterationResult(Base):
    __tablename__ = "hc_iteration_results"

    result_id = Column(Integer, ForeignKey("hc_results.result_id", ondelete="CASCADE"), primary_key=True)
    iteration_id = Column(Integer, ForeignKey("hc_iteration_jobs.id", ondelete="CASCADE"), primary_key=True)
    count = Column(Integer, nullable=False)

    result = relationship("HCResult", back_populates="iteration_results", passive_deletes=True)
    iteration_job = relationship("HCIterationJob", back_populates="iteration_results", passive_deletes=True)

    __table_args__ = (
        Index('idx_hc_iteration_result', 'result_id', 'iteration_id', unique=True),
    )
