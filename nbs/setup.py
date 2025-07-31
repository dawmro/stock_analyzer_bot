import os
import sys
import pathlib

THIS_FILE_PATH = pathlib.Path(__file__).resolve()
NBS_DIR = THIS_FILE_PATH.parent
REPO_DIR = NBS_DIR.parent
DJANGO_BASE_DIR = REPO_DIR / "src"
DJANGO_PROJECT_SETTINGS_NAME = "sab_home"


def init_django(project_name=DJANGO_PROJECT_SETTINGS_NAME):
    """Run administrative tasks."""
    os.chdir(DJANGO_BASE_DIR)
    sys.path.insert(0, str(DJANGO_BASE_DIR))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", f"{project_name}.settings")
    import django
    django.setup()
    



