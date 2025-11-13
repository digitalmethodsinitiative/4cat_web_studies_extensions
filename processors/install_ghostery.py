from __future__ import annotations

import sys
import subprocess
import shutil
import logging
import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal
from datetime import datetime
import json

import requests


REPO_URL = "https://github.com/ghostery/trackerdb.git"
REPO_LATEST_RELEASE = "https://api.github.com/repos/ghostery/trackerdb/releases/latest"
MARKER_FILENAME = ".last_update.json"


@dataclass
class GhosteryUpdateResult:
    # status is authoritative; installer also writes marker internally
    status: Literal[
        "installed",
        "updated",
        "up-to-date",
        "skipped-auto-update-disabled",
        "failed",
        "release-unknown",
    ]
    changed: bool
    latest_release: Optional[str]
    error: Optional[str] = None


def _get_latest_release(logger: logging.Logger) -> Optional[str]:
    try:
        response = requests.get(REPO_LATEST_RELEASE, timeout=30)
    except Exception as e:
        logger.error(f"Ghoserty DB Update Error fetching release: {e}")
        return None

    if response.status_code == 200:
        data = response.json()
        return data.get("tag_name")
    else:
        logger.error(f"Ghoserty DB Update Error fetching release: {response.status_code} - {response.text}")
        return None


def _ensure_node_installed(platform_name: str) -> bool:
    # If node and npm exist, we’re fine
    if shutil.which("node") and shutil.which("npm"):
        return True

    # Only attempt to install automatically on Linux via apt
    if platform_name != "linux":
        raise ValueError(
            "Automatic Node.js/npm installation only supported on Linux. "
            "Install Node.js + npm manually (e.g. via your OS package manager or nvm) and rerun."
        )

    # Best-effort install, may fail if permissions are insufficient
    result = subprocess.run(["apt", "update"], capture_output=True)
    if result.returncode != 0:
        raise ValueError(
            "Error updating apt package lists. You may need elevated permissions (sudo) or a different base image."
        )

    result = subprocess.run(["apt", "install", "-y", "nodejs", "npm"], capture_output=True)
    if result.returncode != 0:
        raise ValueError(
            "Error installing Node.js and npm via apt. "
            "If running inside a container without root, consider installing with nvm (https://github.com/nvm-sh/nvm)."
        )

    return True


def _build_tracker_db(ghostery_repo: Path, trackerdb_file: Path, logger: logging.Logger, platform_name: str) -> bool:
    try:
        _ensure_node_installed(platform_name)
    except ValueError as e:
        logger.warning(f"Node.js preflight skipped/failed: {e}")
        logger.warning(
            "Ghostery tracker database build will likely fail without Node.js. "
            "Install Node.js + npm then rerun the installer: e.g. 'apt install nodejs npm' or use nvm."
        )
        return False

    result = subprocess.run(["npm", "install"], capture_output=True, cwd=str(ghostery_repo))
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace").strip()
        logger.warning(f"npm install failed: {stderr}")
        logger.info(
            "Troubleshooting: ensure package.json exists and Node.js/npm versions are compatible. "
            "Try manually running: 'npm clean-install' or deleting node_modules."
        )
        return False

    result = subprocess.run(["node", "scripts/export-json/index.js"], capture_output=True, cwd=str(ghostery_repo))
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace").strip()
        logger.error(f"Ghostery export script failed: {stderr}")
        logger.info(
            "Verify the repository is current ('git pull'), dependencies installed ('npm install'), "
            "and that scripts/export-json/index.js exists."
        )
        return False

    if not trackerdb_file.exists():
        logger.error("trackerdb.json file not found after export. The build script may have changed upstream.")
        logger.info("Check Ghostery repo 'scripts/export-json' output or build steps in its README.")
        return False

    return True


def _clone_repo(repo_url: str, dest: Path) -> bool:
    result = subprocess.run(["git", "clone", repo_url, str(dest)])
    return result.returncode == 0


def _pull_repo(dest: Path) -> bool:
    result = subprocess.run(["git", "pull"], cwd=str(dest))
    return result.returncode == 0


def _marker_path(path_config: Path) -> Path:
    return path_config.joinpath("ghostery", MARKER_FILENAME)


def read_update_marker(path_config: Path, logger: Optional[logging.Logger] = None) -> Optional[dict]:
    """Read sidecar JSON marker with last known release and timestamps.

    Returns dict or None if missing/invalid.
    { latest_release: str|None, updated_at: str|None, checked_at: str|None, status: str }
    """
    marker = _marker_path(path_config)
    try:
        if not marker.exists():
            return None
        with marker.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        if logger:
            logger.debug(f"Unable to read marker {marker}: {e}")
        return None


def write_update_marker(
    path_config: Path,
    *,
    latest_release: Optional[str],
    status: str,
    updated: bool,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Write sidecar JSON marker to record latest release and timestamps.

    - status: "updated" | "up-to-date" | "failed"
    - updated: True when a build/update occurred, False otherwise
    """
    marker = _marker_path(path_config)
    marker.parent.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.utcnow().isoformat() + "Z"
    payload = {
        "latest_release": latest_release,
        "checked_at": now_iso,
        "updated_at": now_iso if updated else None,
        "status": status,
    }
    try:
        with marker.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        if logger:
            logger.debug(f"Unable to write marker {marker}: {e}")


def install_or_update_ghostery(
    path_ghostery: Path,
    *,
    auto_update: bool,
    current_release: Optional[str],
    logger: logging.Logger,
    platform_name: str = sys.platform,
    repo_url: str = REPO_URL,
) -> GhosteryUpdateResult:
    """Clone/build/update Ghostery trackerdb into <path_ghostery>/dist/trackerdb.json.

    Installer is single source of truth: it writes the sidecar marker in <path_config>/ghostery/.last_update.json.
    """
    trackerdb_file = path_ghostery.joinpath("dist/trackerdb.json")
    path_config = path_ghostery.parent
    path_config.mkdir(parents=True, exist_ok=True)

    # First-time install path
    if not path_ghostery.exists():
        if not auto_update:
            result = GhosteryUpdateResult(
                status="skipped-auto-update-disabled",
                changed=False,
                latest_release=None,
            )
            write_update_marker(path_config, latest_release=None, status=result.status, updated=False, logger=logger)
            return result

        logger.info("Ghostery tracker database not present; cloning and building...")
        latest_release = _get_latest_release(logger)
        if not _clone_repo(repo_url, path_ghostery):
            err = "Error cloning Ghostery tracker database"
            logger.error(err)
            logger.info(
                "Ensure 'git' is installed and you have network access to GitHub. Proxy/firewall may block clone."
            )
            result = GhosteryUpdateResult(
                status="failed",
                changed=False,
                latest_release=latest_release,
                error=err,
            )
            write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=False, logger=logger)
            return result

        if not _build_tracker_db(path_ghostery, trackerdb_file, logger, platform_name):
            err = "Build failed after clone"
            logger.error(err)
            result = GhosteryUpdateResult(
                status="failed",
                changed=False,
                latest_release=latest_release,
                error=err,
            )
            write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=False, logger=logger)
            return result

        result = GhosteryUpdateResult(status="installed", changed=True, latest_release=latest_release)
        write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=True, logger=logger)
        return result

    # Existing repo: decide whether to update
    latest_release = _get_latest_release(logger)
    if not latest_release:
        result = GhosteryUpdateResult(status="release-unknown", changed=False, latest_release=None)
        write_update_marker(path_config, latest_release=None, status=result.status, updated=False, logger=logger)
        return result

    if current_release != latest_release:
        if not auto_update:
            # Inform caller that a new release exists but auto-update is disabled
            result = GhosteryUpdateResult(
                status="skipped-auto-update-disabled",
                changed=False,
                latest_release=latest_release,
            )
            write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=False, logger=logger)
            return result

        logger.info(f"Updating Ghostery tracker database from {current_release or 'unknown'} to {latest_release}")
        if not _pull_repo(path_ghostery):
            err = "Unable to collect updates via git for Ghostery tracker database"
            logger.error(err)
            logger.info(
                "Check if repository has local changes or lacks proper permissions. Try manual 'git fetch --all && git reset --hard origin/main'."
            )
            result = GhosteryUpdateResult(status="failed", changed=False, latest_release=latest_release, error=err)
            write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=False, logger=logger)
            return result

        if not _build_tracker_db(path_ghostery, trackerdb_file, logger, platform_name):
            err = "Build failed after git pull"
            logger.error(err)
            result = GhosteryUpdateResult(status="failed", changed=False, latest_release=latest_release, error=err)
            write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=False, logger=logger)
            return result

        result = GhosteryUpdateResult(status="updated", changed=True, latest_release=latest_release)
        write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=True, logger=logger)
        return result

    # Already current
    result = GhosteryUpdateResult(status="up-to-date", changed=False, latest_release=latest_release)
    write_update_marker(path_config, latest_release=latest_release, status=result.status, updated=False, logger=logger)
    return result

# -----------------------------
# Manual CLI / one-liner support
# -----------------------------

def _make_logger(verbose: bool = False) -> logging.Logger:
    """Create a simple stdout logger for manual runs."""
    logger = logging.getLogger("ghostery-installer")
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(levelname)s: %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger


def quick_install(path_config: Optional[str | Path] = None, *, verbose: bool = False) -> int:
    """Convenience entry point for manual installs.

    Returns 0 on success or already up-to-date, 1 on failure.
    """
    logger = _make_logger(verbose)

    if shutil.which("git") is None:
        logger.error("'git' not found. Please install git and rerun. (e.g. apt install git)")
        return 1

    # Resolve PATH_CONFIG with sensible fallbacks
    cfg = (
        Path(path_config)
        if path_config is not None
        else (
            Path(os.getenv("FOURCAT_PATH_CONFIG"))
            if os.getenv("FOURCAT_PATH_CONFIG")
            else (
                (Path(os.getenv("FOURCAT_PATH_ROOT")) / "config")
                if os.getenv("FOURCAT_PATH_ROOT")
                else (Path.cwd() / "config")
            )
        )
    )
    logger.debug(f"Using PATH_CONFIG: {cfg}")

    # Get last known release from marker to avoid unnecessary rebuilds
    marker = read_update_marker(cfg, logger=logger)
    current_release = marker.get("latest_release") if marker else None

    result = install_or_update_ghostery(
    cfg.joinpath("ghostery"),
        auto_update=True,  # Force action in manual context
        current_release=current_release,
        logger=logger,
        platform_name=sys.platform,
    )

    # Installer already wrote marker; we only log and exit code here
    if result.status in ("installed", "updated"):
        logger.info(f"Ghostery tracker database {result.status}. Release: {result.latest_release}")
        return 0
    if result.status == "up-to-date":
        logger.info(f"Ghostery tracker database up to date. Release: {result.latest_release}")
        return 0
    if result.status == "skipped-auto-update-disabled":
        logger.info(
            "Auto-update disabled; no action taken. If you need elevated permissions, run this installer with sudo/root."
        )
        return 0
    if result.status == "release-unknown":
        logger.warning("Could not determine latest Ghostery release; no action taken.")
        return 0
    logger.error(f"Ghostery installation/update failed: {result.error or 'unknown error'}")
    logger.info(
        "Common fixes: install Node.js/npm; ensure network to GitHub; delete config/ghostery and rerun; update permissions."
    )
    return 1


def cli(argv: Optional[list[str]] = None) -> int:
    """Argument parser wrapper for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Install or update Ghostery tracker database into 4CAT PATH_CONFIG/ghostery."
    )
    parser.add_argument(
        "--path-config",
        type=Path,
        default=None,
        help="4CAT PATH_CONFIG; defaults to $FOURCAT_PATH_CONFIG, then $FOURCAT_PATH_ROOT/config, then ./config.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (DEBUG) logging."
    )
    args = parser.parse_args(argv)
    return quick_install(args.path_config, verbose=args.verbose)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(cli())