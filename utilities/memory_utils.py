import shutil


def get_disk_space():
    """
    Get the current hard disk space available.
    """
    total, used, free = shutil.disk_usage("/")
    return free


def get_total_disk_space():
    """
    Get the total hard disk space available.
    """
    total, used, free = shutil.disk_usage("/")
    return total
