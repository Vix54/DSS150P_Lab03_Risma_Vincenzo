import gc
import statistics
import time


def measure(function, repeats, before=None):
    timings = []
    result = None
    for attempt in range(repeats + 1):
        if before is not None:
            before()
        gc.collect()
        started = time.perf_counter()
        result = function()
        elapsed = time.perf_counter() - started
        if attempt > 0:
            timings.append(elapsed)
    return timings, result


def summarize(timings):
    return {
        'median': statistics.median(timings),
        'min': min(timings),
        'max': max(timings),
        'runs': timings,
    }
