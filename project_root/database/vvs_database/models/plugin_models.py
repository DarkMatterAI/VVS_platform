from sqlalchemy import Column, Integer, String, ForeignKey, JSON, Enum, Table, DateTime, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from vvs_database.core import Base
from vvs_database.schemas.enums import PluginType, PluginExecutionType, PluginClass

plugin_embeddings = Table('plugin_embeddings', Base.metadata,
    Column('plugin_id', Integer, ForeignKey('plugins.id'), primary_key=True),
    Column('embedding_id', Integer, ForeignKey('embedding_plugins.id'), primary_key=True),
)

class Plugin(Base):
    __tablename__ = "plugins"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    plugin_class = Column(Enum(PluginClass), nullable=False)
    type = Column(Enum(PluginType), nullable=False)
    execution_type = Column(Enum(PluginExecutionType), nullable=False)
    
    timeout = Column(Integer, nullable=True)
    max_concurrency = Column(Integer, nullable=True)
    max_retries = Column(Integer, nullable=True)
    batch_size = Column(Integer, nullable=True)
    endpoint_url = Column(String, nullable=True)
    group_key = Column(String, nullable=False)
    config = Column(JSON, nullable=True)
    plugin_metadata = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    embeddings = relationship("EmbeddingPlugin", secondary=plugin_embeddings, back_populates="related_plugins")
    execution_failures = relationship("PluginExecutionFailure", 
                                     back_populates="plugin", 
                                     cascade="all, delete-orphan")
    jobs = relationship("JobPlugin", back_populates="plugin", cascade="all, delete-orphan")
    
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'plugin',
        'polymorphic_load': 'selectin'
    }

class EmbeddingPlugin(Plugin):
    __tablename__ = "embedding_plugins"

    id = Column(Integer, ForeignKey("plugins.id"), primary_key=True)
    vector_length = Column(Integer, nullable=False)
    distance_metric = Column(String(20), nullable=False)

    related_plugins = relationship("Plugin", secondary=plugin_embeddings, back_populates="embeddings")

    __mapper_args__ = {
        'polymorphic_identity': 'embedding',
    }

class DataSourcePlugin(Plugin):
    __tablename__ = "data_source_plugins"

    id = Column(Integer, ForeignKey("plugins.id"), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'data_source',
    }

class FilterPlugin(Plugin):
    __tablename__ = "filter_plugins"

    id = Column(Integer, ForeignKey("plugins.id"), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'filter',
    }

class ScorePlugin(Plugin):
    __tablename__ = "score_plugins"

    id = Column(Integer, ForeignKey("plugins.id"), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'score',
    }

class MapperPlugin(Plugin):
    __tablename__ = "mapper_plugins"

    id = Column(Integer, ForeignKey("plugins.id"), primary_key=True)
    input_embedding_id = Column(Integer, ForeignKey("embedding_plugins.id"), nullable=False)
    output_order = Column(JSON, nullable=False)

    input_embedding = relationship("EmbeddingPlugin", foreign_keys=[input_embedding_id])

    __mapper_args__ = {
        'polymorphic_identity': 'mapper',
    }

    @property
    def output_embeddings(self):
        return [e for e in self.embeddings]

class AssemblyPlugin(Plugin):
    __tablename__ = "assembly_plugins"

    id = Column(Integer, ForeignKey("plugins.id"), primary_key=True)
    num_parents = Column(Integer, nullable=False)

    __mapper_args__ = {
        'polymorphic_identity': 'assembly',
    }

class PluginExecutionFailure(Base):
    __tablename__ = "plugin_execution_failures"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    plugin_id = Column(Integer, ForeignKey("plugins.id", ondelete="CASCADE"), nullable=False)
    job_id = Column(Integer, ForeignKey("vvs_jobs.id", ondelete="CASCADE"), nullable=True)
    failure_reason = Column(String, nullable=True)
    failure_detail = Column(Text, nullable=True) 
    request = Column(JSON, nullable=True)
    
    plugin = relationship("Plugin", back_populates="execution_failures")
    job = relationship("Job", back_populates="execution_failures")


