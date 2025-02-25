class DatabaseError(Exception):
    """Base exception for database errors."""
    pass

class ValidationError(DatabaseError):
    """Raised when validation fails."""
    pass

class NotFoundError(DatabaseError):
    """Raised when a resource is not found."""
    pass

class DuplicateError(DatabaseError):
    """Raised when a unique constraint would be violated."""
    pass

class ReferenceError(DatabaseError):
    """Raised when a reference constraint would be violated."""
    pass