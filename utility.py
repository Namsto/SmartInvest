import os

def check_path(path):
    if os.path.exists(path):
        pass
    else:
        os.makedirs(path)

    return path