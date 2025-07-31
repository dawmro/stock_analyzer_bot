"""
Bootstrap utilities for initializing a Django environment from a stand-alone
notebook, script, or REPL session.

This module is intended to be imported in any *non-web* context (e.g. a
Jupyter notebook, an Airflow task, a management command prototype) where you
need the full Django ORM, settings, and apps registry without running the
traditional `manage.py` entry-point.

Typical usage
-------------
>>> from nbs.setup import init_django
>>> init_django()          # Uses the default project name "sab_home"
>>> from myapp.models import MyModel
>>> MyModel.objects.count()
42
"""

from __future__ import annotations

import os
import sys
import pathlib
from typing import Final

# ---------------------------------------------------------------------------
# Constant definitions
# ---------------------------------------------------------------------------
THIS_FILE_PATH: Final[pathlib.Path] = pathlib.Path(__file__).resolve()
NBS_DIR: Final[pathlib.Path] = THIS_FILE_PATH.parent
REPO_DIR: Final[pathlib.Path] = NBS_DIR.parent
DJANGO_BASE_DIR: Final[pathlib.Path] = REPO_DIR / "src"
DJANGO_PROJECT_SETTINGS_NAME: Final[str] = "sab_home"
"""
Default Django project whose settings module will be imported.

The value is *relative* to DJANGO_BASE_DIR: the effective settings module
will be ``<DJANGO_PROJECT_SETTINGS_NAME>.settings``.
"""

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def init_django(project_name: str = "sab_home") -> None:
    """
    Prepare the Python runtime so that Django can be imported and used.

    This function:

    1. Switches the current working directory to ``DJANGO_BASE_DIR``, ensuring
       that relative paths inside Django settings (e.g. ``BASE_DIR / "templates"``)
       resolve correctly.
    2. Prepends ``DJANGO_BASE_DIR`` to ``sys.path`` so that the project and
       all its apps are importable as top-level packages (e.g.
       ``import sab_home.settings`` instead of fiddling with ``importlib``).
    3. Sets the environment variable ``DJANGO_SETTINGS_MODULE`` to
       ``<project_name>.settings``.  This is the canonical way to tell Django
       which settings file to load.
    4. Calls ``django.setup()``, which populates the apps registry, configures
       logging, and performs other start-up duties normally handled by
       ``manage.py``.

    Parameters
    ----------
    project_name : str, optional
        The Python package name of the Django project whose settings should be
        used.  Defaults to :str: `sab_home`.

    Raises
    ------
    ImportError
        If ``django`` is not available on ``sys.path``.
    django.core.exceptions.ImproperlyConfigured
        If the settings module cannot be imported or is mis-configured.
    """
    # Ensure we run from the directory that contains the Django project.
    os.chdir(DJANGO_BASE_DIR)

    # Guarantee the project is findable without editable installs.
    if str(DJANGO_BASE_DIR) not in sys.path:
        sys.path.insert(0, str(DJANGO_BASE_DIR))

    # Tell Django which settings module to load.
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", f"{project_name}.settings")

    # Import and initialize Django itself.  This must happen only once per
    # process; subsequent calls are idempotent because django.setup() is
    # protected by an internal lock.
    import django  # noqa: E402  (import after path manipulation)
    django.setup()