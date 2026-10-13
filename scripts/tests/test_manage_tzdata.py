import importlib.util
import pathlib
import sys
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "manage_tzdata.py"
SPEC = importlib.util.spec_from_file_location("manage_tzdata", SCRIPT)
manage_tzdata = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = manage_tzdata
SPEC.loader.exec_module(manage_tzdata)


class ManageTzdataTests(unittest.TestCase):
    def test_parse_setup_ini_selects_current_package(self):
        package = manage_tzdata.parse_setup_ini(
            "\n".join(
                [
                    "@ tzdata",
                    'sdesc: "Time Zone Database"',
                    "version: 2026e-1",
                    "install: noarch/release/tzdata/tzdata-2026e-1-noarch.tar.xz 3 "
                    + "A" * 128,
                    "[prev]",
                    "version: 2026d-1",
                    "install: old 1 " + "B" * 128,
                    "@ next-package",
                ]
            )
        )
        self.assertEqual("2026e-1", package.version)
        self.assertEqual("tzdata-2026e-1-noarch.tar.xz", package.filename)
        self.assertEqual(3, package.size)

    def test_metadata_round_trip(self):
        values = {
            "TZDATA_PACKAGE_VERSION": "2026e-1",
            "TZDATA_SETUP_INI_URL": "https://example/setup.ini",
            "TZDATA_MIRROR_ROOT_URL": "https://example",
            "TZDATA_PACKAGE_PATH": "release/tzdata/archive.tar.xz",
            "TZDATA_ARCHIVE_FILENAME": "archive.tar.xz",
            "TZDATA_SHA256": "A" * 64,
            "TZDATA_SHA512": "B" * 128,
        }
        package = manage_tzdata.Package(
            "2026f-1",
            "release/tzdata/tzdata-2026f-1-noarch.tar.xz",
            7,
            "C" * 128,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "tzdata.cmake"
            manage_tzdata.write_metadata(path, values, package, "D" * 64)
            parsed = manage_tzdata.read_metadata(path)
        self.assertEqual("2026f-1", parsed["TZDATA_PACKAGE_VERSION"])
        self.assertEqual("D" * 64, parsed["TZDATA_SHA256"])
        self.assertEqual("C" * 128, parsed["TZDATA_SHA512"])

    def test_mismatch_names_exact_update_command(self):
        archive = b"current"
        metadata = {
            "TZDATA_PACKAGE_VERSION": "2026d-1",
            "TZDATA_PACKAGE_PATH": "old.tar.xz",
            "TZDATA_ARCHIVE_FILENAME": "old.tar.xz",
            "TZDATA_SHA256": manage_tzdata.digest(archive, "sha256"),
            "TZDATA_SHA512": manage_tzdata.digest(archive, "sha512"),
        }
        package = manage_tzdata.Package(
            "2026e-1",
            "noarch/release/tzdata/tzdata-2026e-1-noarch.tar.xz",
            len(archive),
            manage_tzdata.digest(archive, "sha512"),
        )
        with self.assertRaises(manage_tzdata.OutOfDateError) as raised:
            manage_tzdata.compare_repository(metadata, package, archive)
        self.assertIn(
            f"Run exactly: {manage_tzdata.UPDATE_COMMAND}", str(raised.exception)
        )

    def test_upstream_checksum_is_verified(self):
        package = manage_tzdata.Package("2026e-1", "archive", 3, "0" * 128)
        with self.assertRaisesRegex(
            manage_tzdata.TzdataError, "failed the upstream checksum"
        ):
            manage_tzdata.verify_upstream_archive(b"abc", package)

    def test_update_safety_guards(self):
        unsafe_states = [
            {"dirty": True},
            {"branch": "master"},
            {"authenticated": False},
            {"branch_exists": True},
            {"unchanged": True},
        ]
        defaults = {
            "dirty": False,
            "branch": "feature",
            "authenticated": True,
            "branch_exists": False,
            "unchanged": False,
        }
        for override in unsafe_states:
            with self.subTest(override=override):
                state = {**defaults, **override}
                with self.assertRaises(manage_tzdata.TzdataError):
                    manage_tzdata.validate_update_state(**state)

        manage_tzdata.validate_update_state(**defaults)


if __name__ == "__main__":
    unittest.main()
