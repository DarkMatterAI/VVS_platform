
from enum import Enum

class PluginType(str, Enum):
    EMBEDDING = 'embedding'
    DATA_SOURCE = 'data_source'
    FILTER = 'filter'
    SCORE = 'score'
    MAPPER = 'mapper'
    ASSEMBLY = 'assembly'

class PluginClass(str, Enum):
    GENERIC = 'generic'
    INTERNAL_RDKIT = 'internal_rdkit'
    INTERNAL_TEI = 'internal_tei'
    INTERNAL_QDRANT = 'internal_qdrant'
    INTERNAL_TRITON = 'internal_triton'

class PluginExecutionType(str, Enum):
    QUEUE = "queue"
    API = "api"

class DistanceMetric(str, Enum):
    Cosine = 'Cosine'
    Euclid = 'Euclid'
    Dot = 'Dot'

class JobStatus(str, Enum):
    CREATED = 'created'
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETE = 'complete'
    COMPLETE_WITH_ERRORS = 'complete_with_errors'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

class JobType(str, Enum):
    TEST_JOB = 'test_job'
    QDRANT_UPLOAD = 'qdrant_upload'
