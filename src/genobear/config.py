import os


def get_default_workers() -> int:
    """
    Return default workers from GENOBEAR_WORKERS env var or CPU count.

    Downloaders that manage their own workers should not use this.
    """
    env_value = os.getenv("GENOBEAR_WORKERS")
    if env_value is not None:
        try:
            value = int(env_value)
            return max(1, value)
        except ValueError:
            pass
    cpu = os.cpu_count() or 1
    return max(1, cpu)


