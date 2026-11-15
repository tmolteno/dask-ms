"""Invoke maintenance tasks for dask-ms-2.

    invoke --list              # show tasks
    invoke versions            # audit every version marker
    invoke release 0.2.37 --reason "what changed" [--dry-run] [--no-push]

A release must keep every version marker in the repository coherent:

* ``pyproject.toml``      ``version``
* ``daskms/__init__.py``  ``__version__`` (also feeds ``docs/conf.py``)
* ``tbump.toml``          ``current``
* ``uv.lock``             root ``dask-ms-2`` package version
* ``HISTORY.rst``         top section (dated by the release task)
* git                     ``Release X.Y.Z: ...`` commit and annotated tag
  ``vX.Y.Z``

The PyPI publish workflow only triggers on ``v*.*.*`` tags, and the
release pushes the branch and the tag together with ``git push
--atomic``, so the remote can never hold one without the other.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from datetime import date
from pathlib import Path

from invoke.exceptions import Exit
from invoke import task

ROOT = Path(__file__).resolve().parent
PYPROJECT = ROOT / "pyproject.toml"
INIT = ROOT / "daskms" / "__init__.py"
TBUMP = ROOT / "tbump.toml"
HISTORY = ROOT / "HISTORY.rst"
LOCK = ROOT / "uv.lock"

VERSION_RE = r"\d+\.\d+\.\d+"

# (label, path, extract-regex, replace-regex, replacement template)
MARKERS = (
    (
        "pyproject.toml version",
        PYPROJECT,
        re.compile(r'(?m)^version = "([^"]+)"$'),
        re.compile(r'(?m)^version = "[^"]+"$'),
        'version = "{new}"',
    ),
    (
        "daskms/__init__.py __version__",
        INIT,
        re.compile(r'(?m)^__version__ = "([^"]+)"$'),
        re.compile(r'(?m)^__version__ = "[^"]+"$'),
        '__version__ = "{new}"',
    ),
    (
        "tbump.toml current",
        TBUMP,
        re.compile(r'(?m)^current = "([^"]+)"$'),
        re.compile(r'(?m)^current = "[^"]+"$'),
        'current = "{new}"',
    ),
    (
        "uv.lock dask-ms-2",
        LOCK,
        re.compile(r'name = "dask-ms-2"\r?\nversion = "([^"]+)"'),
        re.compile(r'(name = "dask-ms-2"\r?\nversion = ")[^"]+(")'),
        r"\g<1>{new}\g<2>",
    ),
)

RELEASE_FILES = (
    "pyproject.toml",
    "daskms/__init__.py",
    "tbump.toml",
    "uv.lock",
    "HISTORY.rst",
)


def _run(*args: str, check: bool = True) -> str:
    proc = subprocess.run(args, cwd=ROOT, text=True, capture_output=True)
    if proc.returncode != 0 and check:
        raise Exit(
            f"command failed ({proc.returncode}): {' '.join(args)}\n"
            f"{proc.stderr.strip() or proc.stdout.strip()}",
            code=proc.returncode,
        )
    return proc.stdout.strip()


def _git(*args: str, check: bool = True) -> str:
    return _run("git", *args, check=check)


def _read_markers() -> dict[str, str]:
    found = {}
    for label, path, extract, _, _ in MARKERS:
        match = extract.search(path.read_text())
        if not match:
            raise Exit(f"could not find {label} in {path.name}")
        found[label] = match.group(1)
    return found


def _history_heading() -> tuple[str | None, str | None]:
    match = re.search(r"(?m)^(\d+\.\d+\.\d+) \(([^)]*)\)$", HISTORY.read_text())
    return (match.group(1), match.group(2)) if match else (None, None)


def _latest_tag() -> str | None:
    return _git("describe", "--tags", "--abbrev=0", check=False) or None


@task
def versions(c) -> None:
    """Audit every version marker for coherence."""
    markers = _read_markers()
    current = markers["pyproject.toml version"]
    hver, hstate = _history_heading()
    tag = _latest_tag()
    problems = []

    width = max(len(label) for label in markers)

    for label, value in markers.items():
        print(f"{label:<{width}}  {value}")
        if value != current:
            problems.append(f"{label} is {value}, but pyproject.toml says {current}")

    if hver is None:
        print(f"{'HISTORY.rst':<{width}}  (no versioned section)")
        problems.append("HISTORY.rst has no versioned section heading")
    elif hver == current and hstate == "unreleased":
        print(f"{'HISTORY.rst':<{width}}  {hver} (unreleased)")
        problems.append(
            f"HISTORY.rst still marks {hver} unreleased, although the other "
            "markers say it was released"
        )
    elif hver == current:
        print(f"{'HISTORY.rst':<{width}}  {hver} ({hstate})")
    else:
        suffix = "" if hstate == "unreleased" else "  [NOT unreleased]"
        print(f"{'HISTORY.rst':<{width}}  {hver} ({hstate}){suffix}")
        if hstate == "unreleased":
            print(
                f"{'':<{width}}  (pending next release; the release task will date it)"
            )
        else:
            problems.append(
                f"HISTORY.rst top section {hver} ({hstate}) is neither the "
                f"released version ({current}) nor a pending '(unreleased)' one"
            )

    if tag is None:
        print(f"{'latest git tag':<{width}}  (none)")
        problems.append("no git tags found")
    else:
        kind = _git("cat-file", "-t", tag)
        annotated = kind == "tag"
        label = "annotated" if annotated else "lightweight"
        print(f"{'latest git tag':<{width}}  {tag} ({label})")
        if tag != f"v{current}":
            problems.append(f"latest tag {tag} does not match v{current}")
        if not annotated:
            problems.append(
                f"latest tag {tag} is not annotated "
                "(publish workflow needs a v*.*.* tag, releases use annotated tags)"
            )

    if problems:
        raise Exit(
            "version markers are NOT coherent:\n- " + "\n- ".join(problems),
            code=1,
        )

    print(f"OK: all markers coherent at {current}")


@task
def release(
    c,
    new_version: str,
    reason: str = "",
    dry_run: bool = False,
    no_push: bool = False,
) -> None:
    """Release NEW_VERSION: patch every marker, commit, tag, push atomically.

    HISTORY.rst's top section must already be ``NEW_VERSION (unreleased)``
    and committed (CONTRIBUTING step 1); this task dates it, rewrites the
    other markers, commits ``Release NEW_VERSION[: reason]``, tags
    ``vNEW_VERSION`` annotated and pushes branch + tag together.
    """
    if not re.fullmatch(VERSION_RE, new_version):
        raise Exit(f"new version {new_version!r} must look like 0.2.37")

    markers = _read_markers()
    values = set(markers.values())
    resume = values == {new_version}

    if not resume:
        if len(values) > 1:
            detail = "\n- ".join(f"{k}: {v}" for k, v in markers.items())
            raise Exit(
                "version markers already disagree; run `invoke versions` "
                f"and fix them first:\n- {detail}"
            )
        current = values.pop()
        if tuple(map(int, new_version.split("."))) <= tuple(
            map(int, current.split("."))
        ):
            raise Exit(f"{new_version} is not greater than current version {current}")

    branch = _git("branch", "--show-current")
    if branch != "master" and not dry_run:
        raise Exit(f"releases are cut from master, not {branch!r}")
    if branch != "master":
        print(f"warning: not on master (a real release would stop here): {branch}")

    dirty = _git("status", "--porcelain", "--untracked-files=no")
    if dirty and not dry_run:
        raise Exit("tracked changes present; commit or stash them first:\n" + dirty)
    if dirty and dry_run:
        print("warning: tracked changes present (a real release would stop here):")
        print(dirty)

    if _git("tag", "-l", f"v{new_version}"):
        raise Exit(f"tag v{new_version} already exists")

    hver, hstate = _history_heading()
    if hver != new_version:
        raise Exit(
            f"HISTORY.rst top section is {hver} ({hstate}); add "
            f"{new_version} (unreleased) and commit it first "
            "(CONTRIBUTING step 1)"
        )

    if dry_run:
        print("(dry-run: skipping git fetch and origin/master sync check)")
    else:
        _git("fetch", "origin", "--tags")
        if _git("rev-parse", "HEAD") != _git("rev-parse", "origin/master"):
            raise Exit("HEAD and origin/master differ; push or pull first")

    today = date.today().isoformat()
    history_state = today if hstate == "unreleased" else hstate
    heading_old = f"{hver} ({hstate})"
    heading_new = f"{new_version} ({history_state})"
    commit_message = f"Release {new_version}" + (f": {reason}" if reason else "")
    tag_name = f"v{new_version}"
    tag_message = f"dask-ms-2 {new_version}"

    if dry_run:
        print(
            f"Would release {new_version}:" if not resume else "Would finish release:"
        )
        for label, path, _, _, _ in MARKERS:
            value = markers[label]
            if value == new_version:
                print(f"  {label:<30} {value} (already patched)")
            else:
                print(f"  {label:<30} {value} -> {new_version}")
        if heading_old != heading_new:
            print(f"  {'HISTORY.rst heading':<30} {heading_old} -> {heading_new}")
        print(f"  {'uv lock':<30} refresh root package version")
        suffix = "" if reason else '   # hint: pass --reason "what changed"'
        print(f"  {'commit':<30} {commit_message!r}{suffix}")
        print(f"  {'tag':<30} {tag_name} (annotated, message {tag_message!r})")
        if no_push:
            print("  push                      skipped (--no-push)")
        else:
            print(f"  {'push':<30} git push --atomic origin master {tag_name}")
        print("dry-run: nothing was written")
        return

    for label, path, _, replace, template in MARKERS:
        text = path.read_text()
        updated, count = replace.subn(template.format(new=new_version), text, count=1)
        if count != 1:
            raise Exit(
                f"expected exactly one {label} pattern in {path.name}, found {count}"
            )
        path.write_text(updated)

    if heading_old != heading_new:
        text = HISTORY.read_text()
        pattern = re.compile(rf"(?m)^{re.escape(hver)} \([^)]*\)$")
        updated, count = pattern.subn(heading_new, text, count=1)
        if count != 1:
            raise Exit(
                f"expected exactly one {heading_old!r} heading in HISTORY.rst, "
                f"found {count}"
            )
        HISTORY.write_text(updated)

    if shutil.which("uv"):
        _run("uv", "lock")
    else:
        print("warning: uv not on PATH; uv.lock was only patched textually")

    after = _read_markers()
    bad = {label: v for label, v in after.items() if v != new_version}
    if bad:
        raise Exit(f"markers do not all read {new_version} after patching: {bad}")
    hver2, hstate2 = _history_heading()
    if hver2 != new_version or hstate2 == "unreleased":
        raise Exit(
            f"HISTORY.rst heading is {hver2} ({hstate2}), "
            f"expected a dated {new_version} section"
        )

    _git("add", "--", *RELEASE_FILES)
    _git("commit", "-m", commit_message)
    _git("tag", "--annotate", tag_name, "--message", tag_message)

    if no_push:
        print(f"Released {new_version} locally:")
        print(f"  {_git('rev-parse', '--short', 'HEAD')}  {commit_message}")
        print(f"  {tag_name}  (annotated, message {tag_message!r})")
        print(f"push when ready:  git push --atomic origin master {tag_name}")
        return

    _git("push", "--atomic", "origin", "master", tag_name)

    local_tag = _git("rev-parse", f"refs/tags/{tag_name}")
    remote = _git("ls-remote", "--tags", "origin", f"refs/tags/{tag_name}")
    if not remote.startswith(local_tag):
        raise Exit(
            f"remote tag {tag_name} does not match local: {remote!r} vs {local_tag!r}"
        )
    if _git("rev-parse", "HEAD") != _git("rev-parse", "origin/master"):
        raise Exit("origin/master does not match HEAD after the push")

    print(f"Released {new_version}")
    print(f"  commit  {_git('rev-parse', '--short', 'HEAD')}  {commit_message}")
    print(f"  tag     {tag_name} (annotated) -- v*.*.* triggers the PyPI publish")
