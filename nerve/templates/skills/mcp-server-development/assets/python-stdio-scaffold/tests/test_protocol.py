import json, os, subprocess, sys, unittest
from pathlib import Path

ROOT = Path(__file__).parents[1]

class ProtocolTest(unittest.TestCase):
    def test_stdio_round_trip(self):
        env = os.environ | {"PYTHONPATH": str(ROOT / "src")}
        p = subprocess.Popen([sys.executable, "-m", "example_mcp.server"], cwd=ROOT, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        requests = [{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}, {"jsonrpc":"2.0","id":2,"method":"tools/list"}, {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"text":"fixture"}}}]
        stdout, stderr = p.communicate("".join(json.dumps(x)+"\n" for x in requests), timeout=5)
        self.assertEqual(p.returncode, 0, stderr)
        result = [json.loads(line) for line in stdout.splitlines()]
        self.assertEqual([x["id"] for x in result], [1, 2, 3])
        self.assertFalse(result[1]["result"]["tools"][0]["inputSchema"]["additionalProperties"])
        self.assertEqual(result[2]["result"]["content"][0]["text"], "fixture")
