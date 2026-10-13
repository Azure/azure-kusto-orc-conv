#!/usr/bin/env python3
"""Check or update the repository's vendored Cygwin tzdata package."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import pathlib
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
METADATA_RELATIVE = pathlib.Path("third_party/tzdata/tzdata.cmake")
METADATA_PATH = REPO_ROOT / METADATA_RELATIVE
UPDATE_COMMAND = "python scripts/manage_tzdata.py update"
PACKAGE_NAME = "tzdata"
BRANCH_PREFIX = "user/shaharp/update-tzdata"
EXPECTED_ORIGIN = "https://github.com/Azure/azure-kusto-orc-conv.git"
METADATA_KEYS = (
    "TZDATA_PACKAGE_VERSION",
    "TZDATA_SETUP_INI_URL",
    "TZDATA_MIRROR_ROOT_URL",
    "TZDATA_PACKAGE_PATH",
    "TZDATA_ARCHIVE_FILENAME",
    "TZDATA_SHA256",
    "TZDATA_SHA512",
)


class TzdataError(RuntimeError):
    """Base error for actionable tzdata failures."""


class NetworkError(TzdataError):
    """The upstream metadata or archive could not be retrieved."""


class OutOfDateError(TzdataError):
    """The repository copy differs from the canonical package."""


@dataclasses.dataclass(frozen=True)
class Package:
    version: str
    path: str
    size: int
    sha512: str

    @property
    def filename(self) -> str:
        return pathlib.PurePosixPath(self.path).name


def read_metadata(path: pathlib.Path = METADATA_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    pattern = re.compile(r'^set\((TZDATA_[A-Z0-9_]+)\s+"([^"]+)"\)$')
    for line in path.read_text(encoding="utf-8").splitlines():
        match = pattern.fullmatch(line.strip())
        if match:
            values[match.group(1)] = match.group(2)

    missing = [key for key in METADATA_KEYS if key not in values]
    if missing:
        raise TzdataError(
            f"{path} is missing required metadata: {', '.join(missing)}"
        )
    return values


def parse_setup_ini(text: str, package_name: str = PACKAGE_NAME) -> Package:
    marker = f"@ {package_name}"
    lines = text.splitlines()
    try:
        start = lines.index(marker) + 1
    except ValueError as error:
        raise TzdataError(f"Cygwin setup.ini has no {package_name!r} package") from error

    version: str | None = None
    install: str | None = None
    for line in lines[start:]:
        if line.startswith("@ "):
            break
        if line.startswith("["):
            break
        if line.startswith("version: ") and version is None:
            version = line.removeprefix("version: ").strip()
        elif line.startswith("install: ") and install is None:
            install = line.removeprefix("install: ").strip()

    if version is None or install is None:
        raise TzdataError(
            f"Cygwin setup.ini has incomplete current metadata for {package_name!r}"
        )

    fields = install.split()
    if len(fields) != 3:
        raise TzdataError(f"Unexpected setup.ini install record: {install}")
    path, size_text, sha512 = fields
    if not re.fullmatch(r"[0-9a-fA-F]{128}", sha512):
        raise TzdataError(f"Invalid upstream SHA-512 for {package_name}: {sha512}")
    try:
        size = int(size_text)
    except ValueError as error:
        raise TzdataError(f"Invalid upstream package size: {size_text}") from error

    package = Package(version, path, size, sha512.upper())
    if version not in package.filename:
        raise TzdataError(
            f"Package version {version} does not match archive {package.filename}"
        )
    return package


def read_url(url: str) -> bytes:
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            return response.read()
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as error:
        raise NetworkError(f"network error retrieving {url}: {error}") from error


def discover_package(setup_ini_url: str) -> Package:
    data = read_url(setup_ini_url)
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise TzdataError(f"{setup_ini_url} is not valid UTF-8") from error
    return parse_setup_ini(text)


def digest(data: bytes, algorithm: str) -> str:
    return hashlib.new(algorithm, data).hexdigest().upper()


def verify_upstream_archive(data: bytes, package: Package) -> str:
    actual_sha512 = digest(data, "sha512")
    if actual_sha512 != package.sha512:
        raise TzdataError(
            "Downloaded archive failed the upstream checksum: "
            f"expected {package.sha512}, got {actual_sha512}"
        )
    if len(data) != package.size:
        raise TzdataError(
            f"Downloaded archive size differs: expected {package.size}, got {len(data)}"
        )
    return digest(data, "sha256")


def mismatch_message(reason: str) -> str:
    return (
        f"Vendored tzdata is out of date: {reason}\n"
        f"Run exactly: {UPDATE_COMMAND}"
    )


def compare_repository(
    metadata: dict[str, str],
    package: Package,
    archive_data: bytes,
    upstream_data: bytes | None = None,
) -> None:
    differences: list[str] = []
    if metadata["TZDATA_PACKAGE_VERSION"] != package.version:
        differences.append(
            "version "
            f"{metadata['TZDATA_PACKAGE_VERSION']} != upstream {package.version}"
        )
    if metadata["TZDATA_PACKAGE_PATH"] != package.path:
        differences.append(
            f"package path {metadata['TZDATA_PACKAGE_PATH']} != upstream {package.path}"
        )
    if metadata["TZDATA_ARCHIVE_FILENAME"] != package.filename:
        differences.append(
            "archive filename "
            f"{metadata['TZDATA_ARCHIVE_FILENAME']} != upstream {package.filename}"
        )
    if metadata["TZDATA_SHA512"].upper() != package.sha512:
        differences.append("metadata SHA-512 differs from upstream")

    actual_sha256 = digest(archive_data, "sha256")
    if metadata["TZDATA_SHA256"].upper() != actual_sha256:
        differences.append(
            f"vendored SHA-256 {actual_sha256} != metadata "
            f"{metadata['TZDATA_SHA256'].upper()}"
        )
    actual_sha512 = digest(archive_data, "sha512")
    if package.sha512 != actual_sha512:
        differences.append(
            f"vendored SHA-512 {actual_sha512} != upstream {package.sha512}"
        )
    if len(archive_data) != package.size:
        differences.append(
            f"vendored size {len(archive_data)} != upstream {package.size}"
        )
    if upstream_data is not None:
        upstream_sha256 = digest(upstream_data, "sha256")
        if metadata["TZDATA_SHA256"].upper() != upstream_sha256:
            differences.append(
                f"metadata SHA-256 differs from downloaded upstream {upstream_sha256}"
            )
        if archive_data != upstream_data:
            differences.append("vendored bytes differ from downloaded upstream bytes")

    if differences:
        raise OutOfDateError(mismatch_message("; ".join(differences)))


def run(
    command: list[str],
    *,
    cwd: pathlib.Path = REPO_ROOT,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise TzdataError(f"{' '.join(command)} failed: {detail}")
    return result


def validate_update_state(
    *,
    dirty: bool,
    branch: str,
    authenticated: bool,
    branch_exists: bool,
    unchanged: bool,
) -> None:
    if dirty:
        raise TzdataError("Refusing update from a dirty worktree")
    if branch == "master":
        raise TzdataError("Refusing update while running on master")
    if not branch:
        raise TzdataError("Refusing update from a detached HEAD")
    if not authenticated:
        raise TzdataError("GitHub CLI authentication is required; run gh auth login")
    if branch_exists:
        raise TzdataError("Refusing to overwrite an existing update branch")
    if unchanged:
        raise TzdataError("Vendored tzdata already matches the current Cygwin package")


def write_metadata(
    path: pathlib.Path,
    old_metadata: dict[str, str],
    package: Package,
    sha256: str,
) -> None:
    values = {
        **old_metadata,
        "TZDATA_PACKAGE_VERSION": package.version,
        "TZDATA_PACKAGE_PATH": package.path,
        "TZDATA_ARCHIVE_FILENAME": package.filename,
        "TZDATA_SHA256": sha256,
        "TZDATA_SHA512": package.sha512,
    }
    lines = ["# Source of truth for the vendored Cygwin tzdata package."]
    lines.extend(f'set({key} "{values[key]}")' for key in METADATA_KEYS)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def check_freshness(args: argparse.Namespace) -> int:
    metadata = read_metadata()
    setup_ini_url = args.setup_ini_url or metadata["TZDATA_SETUP_INI_URL"]
    package = discover_package(setup_ini_url)
    archive_url = (
        metadata["TZDATA_MIRROR_ROOT_URL"].rstrip("/") + "/" + package.path
    )
    upstream_data = read_url(archive_url)
    verify_upstream_archive(upstream_data, package)

    archive_path = REPO_ROOT / "third_party" / "tzdata" / metadata[
        "TZDATA_ARCHIVE_FILENAME"
    ]
    if not archive_path.is_file():
        raise OutOfDateError(
            mismatch_message(f"vendored archive is missing: {archive_path}")
        )
    compare_repository(
        metadata, package, archive_path.read_bytes(), upstream_data
    )
    print(
        f"Vendored tzdata {package.version} is current "
        f"(SHA-256 {metadata['TZDATA_SHA256']})."
    )
    return 0


def branch_exists(branch: str) -> bool:
    local = run(
        ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        check=False,
    )
    if local.returncode == 0:
        return True
    if local.returncode != 1:
        raise TzdataError("Unable to inspect local branches")
    remote = run(
        ["git", "ls-remote", "--exit-code", "--heads", "origin", branch],
        check=False,
    )
    if remote.returncode == 0:
        return True
    if remote.returncode != 2:
        detail = remote.stderr.strip() or remote.stdout.strip()
        raise TzdataError(f"Unable to inspect remote branches: {detail}")
    return False


def update_tzdata(_args: argparse.Namespace) -> int:
    metadata = read_metadata()
    git_root = pathlib.Path(
        run(["git", "rev-parse", "--show-toplevel"]).stdout.strip()
    ).resolve()
    if git_root != REPO_ROOT:
        raise TzdataError(f"Run the update from repository root {REPO_ROOT}")

    origin = run(["git", "remote", "get-url", "origin"]).stdout.strip()
    if origin.rstrip("/") not in (
        EXPECTED_ORIGIN,
        "git@github.com:Azure/azure-kusto-orc-conv.git",
    ):
        raise TzdataError(f"Unexpected origin remote: {origin}")

    dirty = bool(run(["git", "status", "--porcelain"]).stdout.strip())
    current_branch = run(
        ["git", "branch", "--show-current"]
    ).stdout.strip()
    authenticated = (
        shutil.which("gh") is not None
        and run(["gh", "auth", "status"], check=False).returncode == 0
    )

    validate_update_state(
        dirty=dirty,
        branch=current_branch,
        authenticated=authenticated,
        branch_exists=False,
        unchanged=False,
    )

    run(["git", "fetch", "origin", "master", "--prune"])
    package = discover_package(metadata["TZDATA_SETUP_INI_URL"])
    branch = f"{BRANCH_PREFIX}-{package.version}"
    exists = branch_exists(branch)
    unchanged = (
        metadata["TZDATA_PACKAGE_VERSION"] == package.version
        and metadata["TZDATA_PACKAGE_PATH"] == package.path
        and metadata["TZDATA_SHA512"].upper() == package.sha512
    )
    validate_update_state(
        dirty=False,
        branch=current_branch,
        authenticated=True,
        branch_exists=exists,
        unchanged=unchanged,
    )

    archive_url = (
        metadata["TZDATA_MIRROR_ROOT_URL"].rstrip("/") + "/" + package.path
    )
    archive_data = read_url(archive_url)
    sha256 = verify_upstream_archive(archive_data, package)
    current_archive = (
        REPO_ROOT / "third_party" / "tzdata" / metadata["TZDATA_ARCHIVE_FILENAME"]
    )
    if current_archive.is_file() and current_archive.read_bytes() == archive_data:
        raise TzdataError("Refusing update because the archive bytes are unchanged")

    worktree = REPO_ROOT.parent / (
        f"{REPO_ROOT.name}-tzdata-{package.version.replace('/', '-')}"
    )
    if worktree.exists():
        raise TzdataError(f"Refusing to reuse existing update path: {worktree}")

    run(["git", "worktree", "add", "-b", branch, str(worktree), "origin/master"])
    completed = False
    try:
        target_dir = worktree / "third_party" / "tzdata"
        target_metadata = target_dir / METADATA_PATH.name
        target_dir.mkdir(parents=True, exist_ok=True)
        old_archive = target_dir / metadata["TZDATA_ARCHIVE_FILENAME"]
        if old_archive.name != package.filename and old_archive.exists():
            old_archive.unlink()
        (target_dir / package.filename).write_bytes(archive_data)
        write_metadata(target_metadata, metadata, package, sha256)

        run(["git", "add", "-A", str(METADATA_RELATIVE.parent)], cwd=worktree)
        staged = run(["git", "diff", "--cached", "--quiet"], cwd=worktree, check=False)
        if staged.returncode == 0:
            raise TzdataError("Refusing update because no repository files changed")
        if staged.returncode != 1:
            raise TzdataError("Unable to inspect the staged tzdata update")

        run(
            [
                "git",
                "commit",
                "-m",
                f"Vendor Cygwin tzdata {package.version}",
                "-m",
                "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>",
            ],
            cwd=worktree,
        )
        run(["git", "push", "-u", "origin", branch], cwd=worktree)
        upstream = run(
            [
                "git",
                "rev-parse",
                "--abbrev-ref",
                "--symbolic-full-name",
                "@{upstream}",
            ],
            cwd=worktree,
        ).stdout.strip()
        if upstream != f"origin/{branch}":
            raise TzdataError(f"Unexpected upstream tracking branch: {upstream}")

        body = (
            f"Updates the vendored Cygwin tzdata package to {package.version}.\n\n"
            "The archive was discovered through Cygwin setup.ini and verified "
            "against its upstream SHA-512 before being committed."
        )
        pr_url = run(
            [
                "gh",
                "pr",
                "create",
                "--repo",
                "Azure/azure-kusto-orc-conv",
                "--draft",
                "--base",
                "master",
                "--head",
                branch,
                "--title",
                f"Vendor Cygwin tzdata {package.version}",
                "--body",
                body,
            ],
            cwd=worktree,
        ).stdout.strip()
        completed = True
        print(f"Created {pr_url}")
        return 0
    finally:
        if completed:
            run(["git", "worktree", "remove", str(worktree)])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    check_parser = subparsers.add_parser(
        "check", help="compare the repository archive with current Cygwin metadata"
    )
    check_parser.add_argument(
        "--setup-ini-url",
        help="override setup.ini URL (intended for deterministic tests)",
    )
    check_parser.set_defaults(handler=check_freshness)
    update_parser = subparsers.add_parser(
        "update", help="create, push, and open a PR for the current Cygwin package"
    )
    update_parser.set_defaults(handler=update_tzdata)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except NetworkError as error:
        print(f"NETWORK ERROR: {error}", file=sys.stderr)
        return 2
    except OutOfDateError as error:
        print(error, file=sys.stderr)
        return 1
    except TzdataError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
