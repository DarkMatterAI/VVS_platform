
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
    COMPLETE_EARLY_STOP = 'complete_early_stop'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

TERMINAL_STATUSES = set([
    JobStatus.COMPLETE, 
    JobStatus.COMPLETE_WITH_ERRORS, 
    JobStatus.COMPLETE_EARLY_STOP,
    JobStatus.FAILED,
    JobStatus.CANCELLED
])

class JobType(str, Enum):
    TEST_JOB = 'test_job'
    QDRANT_UPLOAD = 'qdrant_upload'
    HILL_CLIMB_JOB = 'hill_climb_job'
    HILL_CLIMB_JOB_INPUT = 'hill_climb_job_input'
    HILL_CLIMB_JOB_ITERATION = 'hill_climb_job_iteration'
