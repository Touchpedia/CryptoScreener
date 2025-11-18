START_TS = None   # type: float | None
IS_RUNNING = False

def set_started(ts):
    global START_TS, IS_RUNNING
    START_TS = ts
    IS_RUNNING = True

def clear_started():
    global START_TS, IS_RUNNING
    START_TS = None
    IS_RUNNING = False

def get_started():
    return START_TS

def is_running():
    return IS_RUNNING