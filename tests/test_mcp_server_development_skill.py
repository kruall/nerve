"""Contract tests for the standalone-MCP development skill."""
import shutil
import subprocess
import sys
from pathlib import Path

from nerve.skills.manager import validate_skill_package
from nerve.workspace import install_bundled_skills


ROOT = Path(__file__).parents[1]
SKILL = ROOT / "nerve" / "templates" / "skills" / "mcp-server-development"


def test_skill_is_canonical_and_bundled(tmp_path):
    installed = install_bundled_skills(tmp_path)
    assert "mcp-server-development" in installed
    raw = (tmp_path / "skills" / "mcp-server-development" / "SKILL.md").read_text()
    package = validate_skill_package(raw, "mcp-server-development", allow_legacy=False)
    assert package.version == "1.0.0"
    assert "Gate before repository or worktree selection" in package.body


def test_copied_scaffold_passes_stdio_round_trip(tmp_path):
    target = tmp_path / "example-mcp"
    shutil.copytree(SKILL / "assets" / "python-stdio-scaffold", target)
    result = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"], cwd=target, text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_placement_gate_distinguishes_generic_and_nerve_owned_work():
    skill = (SKILL / "SKILL.md").read_text()
    tests = (SKILL / "references" / "forward-tests.md").read_text()
    nerve_dev = (ROOT / "nerve" / "templates" / "skills" / "nerve-dev" / "SKILL.md").read_text()
    assert "Detached\nexecutions" in skill
    assert all(term in tests for term in ("compiler", "Git", "third-party", "session cancellation"))
    assert "Using a capability through Nerve is not a reason" in nerve_dev
