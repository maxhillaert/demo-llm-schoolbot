

def task(func):
    """Factory function to create a task."""
    def wrapped_task(*args, **kwargs):
        print(f"Running task: {func.__name__}")
        return func(*args, **kwargs)
    return wrapped_task


def cached_task(task_func, compute_condition_func):
    """Factory function to create a conditional task."""
    def wrapped_func(*args, **kwargs):
        existing_resource = compute_condition_func()
        if existing_resource is None:
            return task_func(*args, **kwargs)
        else:
            print(f"Skipping task: {task_func.__name__}")
            return existing_resource
    return wrapped_func
