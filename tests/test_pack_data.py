"""scripts/pack_data.sh — упаковка данных без гигабайтных снимков."""
from __future__ import annotations

import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SH = ROOT / "scripts" / "pack_data.sh"


class PackTests(unittest.TestCase):
    def _run(self, work: Path, out: Path):
        # Запускаем КОПИЮ в песочнице: скрипт переходит в свою папку (dirname/..),
        # и оригинал паковал бы настоящие данные репозитория, а не фикстуру.
        return subprocess.run(["bash", str(work / "scripts" / "pack_data.sh"), str(out)],
                              cwd=work, capture_output=True, text=True)

    def _fixture(self) -> Path:
        d = Path(tempfile.mkdtemp())
        (d / "scripts").mkdir()
        (d / "scripts" / "pack_data.sh").write_text(SH.read_text())
        (d / "docs").mkdir()
        (d / "docs" / "HANDOFF.md").write_text("передача\n")
        for name in ("data_fv_lag", "data_probe"):
            p = d / name
            p.mkdir()
            (p / "trades.csv").write_text("a,b\n1,2\n")
            (p / "outcomes.csv").write_text("slug,winner\ns,Up\n")
            (p / "state.json").write_text("{}")
            (p / "snapshots.csv").write_text("x" * 5000)      # «гигабайты»
        (d / "data_fill").mkdir()
        (d / "data_fill" / "survive.csv").write_text("ask0\n0.6\n")
        return d

    def test_packs_the_useful_files(self):
        work = self._fixture()
        out = work / "out.tgz"
        r = self._run(work, out)
        self.assertEqual(r.returncode, 0, r.stderr)
        names = tarfile.open(out).getnames()
        self.assertIn("data_fv_lag/trades.csv", names)
        self.assertIn("data_fv_lag/state.json", names)
        self.assertIn("data_probe/outcomes.csv", names)
        self.assertIn("data_fill/survive.csv", names)
        self.assertIn("docs/HANDOFF.md", names)

    def test_snapshots_are_excluded(self):
        work = self._fixture()
        out = work / "out.tgz"
        self._run(work, out)
        for n in tarfile.open(out).getnames():
            self.assertNotIn("snapshots", n, "снимки рынка попали в архив")

    def test_reports_size_and_contents(self):
        work = self._fixture()
        r = self._run(work, work / "out.tgz")
        self.assertIn("Размер:", r.stdout)
        self.assertIn("Что внутри:", r.stdout)

    def test_says_so_when_there_is_nothing(self):
        d = Path(tempfile.mkdtemp())
        (d / "scripts").mkdir()
        (d / "scripts" / "pack_data.sh").write_text(SH.read_text())
        (d / "docs").mkdir()
        r = self._run(d, d / "out.tgz")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("Нечего упаковывать", r.stderr)

    def test_default_path_is_in_home(self):
        self.assertIn('${1:-$HOME/polydata.tgz}', SH.read_text())


if __name__ == "__main__":
    unittest.main()
