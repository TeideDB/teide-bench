"""Shared utilities for building engines from custom directories or branches."""

import os
import subprocess
import shutil

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_PIP = os.path.join(SCRIPT_DIR, ".venv", "bin", "pip")

ENGINE_REPOS = {
    "rayforce": "https://github.com/RayforceDB/rayforce.git",
    "teide": "https://github.com/TeideDB/teide.git",
}


def git_info(directory):
    """Get branch, short commit, and dirty flag from a directory."""
    branch = ""
    commit = ""
    dirty = False
    try:
        branch = subprocess.check_output(
            ["git", "-C", directory, "branch", "--show-current"],
            stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        pass
    try:
        commit = subprocess.check_output(
            ["git", "-C", directory, "log", "--oneline", "-1", "--no-decorate"],
            stderr=subprocess.DEVNULL).decode().strip()[:12]
    except Exception:
        pass
    try:
        status = subprocess.check_output(
            ["git", "-C", directory, "status", "--porcelain"],
            stderr=subprocess.DEVNULL).decode().strip()
        dirty = len(status) > 0
    except Exception:
        pass
    return branch, commit, dirty


def resolve_source(engine, src_dir=None, branch=None):
    """Resolve engine source: use src_dir if given, or clone branch from repo.
    Returns directory path."""
    if src_dir:
        return os.path.abspath(src_dir)
    if branch:
        repo = ENGINE_REPOS.get(engine)
        if not repo:
            raise ValueError(f"No repo configured for {engine}")
        clone_dir = os.path.join(SCRIPT_DIR, ".deps", f"{engine}-branch-{branch}")
        if os.path.exists(clone_dir):
            subprocess.run(["git", "-C", clone_dir, "fetch", "-q", "origin"],
                           check=True, capture_output=True)
            subprocess.run(["git", "-C", clone_dir, "checkout", "-q", branch],
                           capture_output=True)
            subprocess.run(["git", "-C", clone_dir, "reset", "-q", "--hard",
                            f"origin/{branch}"], capture_output=True)
        else:
            subprocess.run(["git", "clone", "--depth", "1", "-b", branch,
                            repo, clone_dir], check=True, capture_output=True)
        print(f"  Resolved {engine} branch '{branch}' -> {clone_dir}")
        return clone_dir
    return None


def build_engine(engine, src_dir):
    """Build engine from source directory and install into venv."""
    if engine == "rayforce":
        deps = os.path.join(SCRIPT_DIR, ".deps", "rayforce-py")
        tmp_c = os.path.join(deps, "tmp", "rayforce-c")
        if os.path.exists(tmp_c):
            subprocess.run(["rm", "-rf", tmp_c])
        os.makedirs(os.path.dirname(tmp_c), exist_ok=True)
        subprocess.run(["cp", "-r", src_dir, tmp_c], check=True)
        core_dst = os.path.join(deps, "rayforce", "rayforce")
        subprocess.run(["cp", "-r", os.path.join(tmp_c, "core"), core_dst], check=True)
        subprocess.run(["make", "patch_rayforce_makefile", "rayforce_binaries"],
                       cwd=deps, check=True, capture_output=True)
        subprocess.run([VENV_PIP, "install", "-q", "--no-cache-dir",
                        "--force-reinstall", deps], check=True, capture_output=True)
        print(f"  Built rayforce from {src_dir}")

    elif engine == "teide":
        deps_py = os.path.join(SCRIPT_DIR, ".deps", "teide-py")
        vendor = os.path.join(deps_py, "vendor", "teide")
        if os.path.exists(vendor):
            subprocess.run(["rm", "-rf", vendor])
        subprocess.run(["cp", "-r", src_dir, vendor], check=True)
        subprocess.run([VENV_PIP, "install", "-q", "--no-cache-dir",
                        "--force-reinstall", deps_py], check=True, capture_output=True)
        print(f"  Built teide from {src_dir}")


def engine_label(engine, src_dir):
    """Create label like 'rayforce@sort (abc123) dirty'."""
    if not src_dir:
        return engine
    branch, commit, dirty = git_info(src_dir)
    parts = [engine]
    if branch:
        parts[0] += f"@{branch}"
    if commit:
        parts.append(f"({commit})")
    if dirty:
        parts.append("dirty")
    return " ".join(parts)
