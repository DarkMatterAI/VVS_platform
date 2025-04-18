from sqlalchemy import (
    Column, 
    Integer, 
    ForeignKey, 
    JSON, 
    Boolean,
    String,
    DateTime,
    func,
    and_,
    select,
    Index
)

from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.asyncio import AsyncSession

from vvs_database.core import Base
from vvs_database.models.job_models.job_models import Job

class HCJob(Job):
    __tablename__ = "hc_jobs"

    id = Column(Integer, ForeignKey("vvs_jobs.id"), primary_key=True)
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

    id = Column(Integer, ForeignKey("vvs_jobs.id"), primary_key=True)
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

class HCInputItems(Base):
    __tablename__ = "hc_input_items"
    
    job_id = Column(Integer, ForeignKey("hc_input_jobs.id", ondelete="CASCADE"), primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id", ondelete="CASCADE"), primary_key=True)
    assembly_index = Column(Integer, primary_key=True)
    external_id = Column(String, nullable=True)
    
    input_job = relationship("HCInputJob", back_populates="input_items")
    item = relationship("Item")

class HCIterationJob(Job):
    __tablename__ = "hc_iteration_jobs"

    id = Column(Integer, ForeignKey("vvs_jobs.id"), primary_key=True)
    input_id = Column(Integer, ForeignKey("hc_input_jobs.id", ondelete="CASCADE"), nullable=False)
    parent_id = Column(Integer, ForeignKey("hc_iteration_jobs.id"), nullable=True)
    iteration = Column(Integer, nullable=False)
    inference = Column(Integer, nullable=True)
    query_embedding = Column(JSON, nullable=True)

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

    # Index for faster lookups with handling for nullable assembly_id
    __table_args__ = (
        Index('idx_hc_result_uniqueness', 'job_id', 'item_id', 'assembly_id', unique=True),
    )

    # @classmethod
    # async def get_or_create(cls, session: AsyncSession, job_id: int, item_id: int, 
    #                        assembly_id: Optional[int], valid: bool):
    #     """Get existing result or create a new one"""
    #     if assembly_id is None:
    #         stmt = select(cls).where(
    #             and_(
    #                 cls.job_id == job_id,
    #                 cls.item_id == item_id,
    #                 cls.assembly_id == None
    #             )
    #         )
    #     else:
    #         stmt = select(cls).where(
    #             and_(
    #                 cls.job_id == job_id,
    #                 cls.item_id == item_id,
    #                 cls.assembly_id == assembly_id
    #             )
    #         )
            
    #     result = await session.execute(stmt)
    #     existing = result.scalar_one_or_none()
        
    #     if existing:
    #         return existing
            
    #     # Create new result
    #     new_result = cls(job_id=job_id, item_id=item_id, assembly_id=assembly_id, valid=valid)
    #     session.add(new_result)
    #     await session.flush()  # Flush to get the result_id
        
    #     return new_result

class HCIterationResult(Base):
    __tablename__ = "hc_iteration_results"

    result_id = Column(Integer, ForeignKey("hc_results.result_id", ondelete="CASCADE"), primary_key=True)
    iteration_id = Column(Integer, ForeignKey("hc_iteration_jobs.id", ondelete="CASCADE"), primary_key=True)

    result = relationship("HCResult", back_populates="iteration_results", passive_deletes=True)
    iteration_job = relationship("HCIterationJob", back_populates="iteration_results", passive_deletes=True)

    __table_args__ = (
        Index('idx_hc_iteration_result', 'result_id', 'iteration_id', unique=True),
    )

    # @classmethod
    # async def create(cls, session: AsyncSession, result_id: int, iteration_id: int):
    #     """Create a new iteration result or return existing"""
    #     stmt = select(cls).where(
    #         and_(
    #             cls.result_id == result_id,
    #             cls.iteration_id == iteration_id
    #         )
    #     )
    #     result = await session.execute(stmt)
    #     existing = result.scalar_one_or_none()
        
    #     if existing:
    #         return existing
            
    #     # Create new iteration result
    #     new_result = cls(result_id=result_id, iteration_id=iteration_id)
    #     session.add(new_result)
    #     await session.flush()
        
    #     return new_result