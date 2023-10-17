import unittest
from uuid import uuid4

from acacore.models.file import Action
from acacore.models.file import File


class TestFileActionEnum(unittest.TestCase):
    def setUp(self):
        self.file = File(
            id=1,
            uuid=uuid4(),
            checksum="abc123",
            puid="fmt/18",
            relative_path="test.txt",
            is_binary=False,
            file_size_in_bytes=1024,
            signature="test",
        )

    def test_action_enum_values(self):
        assert Action.CONVERT.value == "Convertool: To convert."
        assert Action.REPLACE.value == "Convertool: Replace with template. File is not preservable."
        assert Action.MANUAL.value == (
            "Manual: File should be converted manually. [info about the manual conversion " "from reference_files]."
        )
        assert Action.RENAME.value == "Renamer: File has extension mismatch. Should be renamed"

    def test_file_action(self):
        self.file.action = Action.CONVERT
        assert self.file.action == Action.CONVERT
        assert self.file.action.value == "Convertool: To convert."

        self.file.action = Action.REPLACE
        assert self.file.action == Action.REPLACE
        assert self.file.action.value == "Convertool: Replace with template. File is not preservable."

        self.file.action = Action.MANUAL
        assert self.file.action == Action.MANUAL
        assert (
            self.file.action.value
            == "Manual: File should be converted manually. [info about the manual conversion from reference_files]."
        )

        self.file.action = Action.RENAME
        assert self.file.action == Action.RENAME
        assert self.file.action.value == "Renamer: File has extension mismatch. Should be renamed"


if __name__ == "__main__":
    unittest.main()
