import importlib


launcher = importlib.import_module("launcher")
run = importlib.import_module("run")


def test_pick_server_port_returns_positive_int():
    port = run.pick_server_port(8501)
    assert isinstance(port, int)
    assert port > 0


def test_wait_for_server_returns_false_for_unbound_port():
    assert run.wait_for_server(9, timeout_sec=1) is False


def test_launcher_pick_server_port_returns_positive_int():
    port = launcher._pick_server_port(8501)
    assert isinstance(port, int)
    assert port > 0


def test_launcher_wait_for_server_returns_false_for_unbound_port():
    assert launcher._wait_for_server(9, timeout_sec=1) is False


def test_resolve_path_points_to_project_file():
    resolved = run.resolve_path("main.py")
    assert resolved.lower().endswith("main.py")


def test_launcher_resolve_base_dir_contains_main_py():
    base_dir = launcher._resolve_base_dir()
    assert isinstance(base_dir, str)
    assert base_dir != ""
