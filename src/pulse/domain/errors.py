class PulseError(Exception):
    """Base class for domain errors."""


class InvalidQueryError(PulseError):
    pass


class NotFoundError(PulseError):
    pass


class EventRejectedError(PulseError):
    pass
