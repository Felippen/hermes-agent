import os
import subprocess

from tools.ao_bridge import AOBridge


def test_bridge_env_prepends_codex_shim(tmp_path, monkeypatch):
    shim_dir = tmp_path / "shims"
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

    bridge = AOBridge(
        codex_shim_dir=shim_dir,
        codex_real_bin="/opt/test/bin/codex",
    )

    env = bridge._bridge_env()

    assert env["PATH"].split(os.pathsep)[0] == str(shim_dir)
    assert env["CODEX_REAL_BIN"] == "/opt/test/bin/codex"
    assert env["HOME"] == "/Users/felipelamartine"


def test_bridge_env_does_not_use_shim_as_real_codex(tmp_path, monkeypatch):
    home = tmp_path / "home"
    user_bin = home / "bin"
    shim_dir = tmp_path / "shims"
    user_bin.mkdir(parents=True)
    shim_dir.mkdir()
    shim = shim_dir / "codex"
    shim.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    user_codex = user_bin / "codex"
    user_codex.symlink_to(shim)
    monkeypatch.setenv("PATH", f"{user_bin}{os.pathsep}/usr/bin")
    monkeypatch.delenv("CODEX_REAL_BIN", raising=False)

    bridge = AOBridge(home=str(home), codex_shim_dir=shim_dir)

    assert bridge._bridge_env()["CODEX_REAL_BIN"] == "/opt/homebrew/bin/codex"


def test_ensure_codex_shim_on_user_path_is_non_destructive(tmp_path):
    home = tmp_path / "home"
    shim_dir = tmp_path / "shims"
    shim_dir.mkdir()
    shim = shim_dir / "codex"
    shim.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    bridge = AOBridge(
        home=str(home),
        codex_shim_dir=shim_dir,
        codex_real_bin="/opt/test/bin/codex",
    )
    bridge._ensure_codex_shim_on_user_path()

    user_codex = home / "bin" / "codex"
    assert user_codex.is_symlink()
    assert user_codex.resolve() == shim

    user_codex.unlink()
    user_codex.write_text("existing", encoding="utf-8")
    bridge._ensure_codex_shim_on_user_path()

    assert user_codex.read_text(encoding="utf-8") == "existing"


def test_codex_shim_translates_ao_approval_mode(tmp_path):
    real_codex = tmp_path / "real-codex"
    real_codex.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$@\"\n",
        encoding="utf-8",
    )
    real_codex.chmod(0o755)

    shim = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "tools",
        "ao_shims",
        "codex",
    )
    proc = subprocess.run(
        [
            shim,
            "--approval-mode",
            "full-auto",
            "--model",
            "gpt-5.5",
            "--",
            "hello",
        ],
        text=True,
        capture_output=True,
        env={"CODEX_REAL_BIN": str(real_codex), "PATH": os.environ.get("PATH", "")},
        check=True,
    )

    assert proc.stdout.splitlines() == [
        "--ask-for-approval",
        "never",
        "--sandbox",
        "danger-full-access",
        "--model",
        "gpt-5.5",
        "--",
        "hello",
    ]
