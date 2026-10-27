import stat
import tempfile
import unittest
from pathlib import Path

from pinnacle_credentials import PinnacleCredentialStore


class PinnacleCredentialStoreTests(unittest.TestCase):
    def test_saves_and_restores_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = PinnacleCredentialStore(Path(tmp) / "credentials.enc")
            store.save(" demo-user ", " demo password ")

            self.assertEqual(store.load(), {
                "username": "demo-user",
                "password": " demo password ",
            })

    def test_file_is_encrypted_and_private(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "credentials.enc"
            username, password = "private-user-42", "private-password-xyz"
            store = PinnacleCredentialStore(path)
            store.save(username, password)

            ciphertext = path.read_bytes()
            self.assertNotIn(username.encode(), ciphertext)
            self.assertNotIn(password.encode(), ciphertext)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(path.with_name("credentials.key").stat().st_mode), 0o600)

    def test_rejects_empty_credentials_without_creating_store(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "credentials.enc"
            store = PinnacleCredentialStore(path)

            for username, password in (("", "secret"), ("  ", "secret"), ("user", "")):
                with self.subTest(username=username, password=password):
                    with self.assertRaises(ValueError):
                        store.save(username, password)

            self.assertFalse(path.exists())

    def test_missing_or_invalid_payload_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "credentials.enc"
            store = PinnacleCredentialStore(path)
            self.assertIsNone(store.load())

            store._store.save({"username": "user", "password": ""})
            self.assertIsNone(store.load())


if __name__ == "__main__":
    unittest.main()
