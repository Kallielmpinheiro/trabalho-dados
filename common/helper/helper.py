def safe(func, default=None):
    try:
        return func()
    except Exception:
        return default
