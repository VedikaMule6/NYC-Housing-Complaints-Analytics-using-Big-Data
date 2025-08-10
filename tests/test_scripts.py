import os
import importlib.util


def test_fetch_and_upload_exists():
    assert os.path.exists("scripts/fetch_and_upload.py")


def test_etl_exists():
    assert os.path.exists("scripts/etl.py")

def test_import_fetch_script():
    spec = importlib.util.spec_from_file_location("fetch_and_upload", "scripts/fetch_and_upload.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

# def test_import_etl_script():
#     spec = importlib.util.spec_from_file_location("etl", "scripts/etl.py")
#     module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(module)
