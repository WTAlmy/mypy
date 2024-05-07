"""Interfaces for accessing metadata.

We provide two implementations.
 * The "classic" file system implementation, which uses a directory
   structure of files.
 * A hokey sqlite backed implementation, which basically simulates
   the file system in an effort to work around poor file system performance
   on OS X.
"""

from __future__ import annotations

import binascii
import os
import time
from abc import abstractmethod
from typing import Iterable

import psycopg2
from psycopg2.extras import RealDictCursor


class MetadataStore:
    """Generic interface for metadata storage."""

    @abstractmethod
    def getmtime(self, name: str) -> float:
        """Read the mtime of a metadata entry..

        Raises FileNotFound if the entry does not exist.
        """

    @abstractmethod
    def read(self, name: str) -> str:
        """Read the contents of a metadata entry.

        Raises FileNotFound if the entry does not exist.
        """

    @abstractmethod
    def write(self, name: str, data: str, mtime: float | None = None) -> bool:
        """Write a metadata entry.

        If mtime is specified, set it as the mtime of the entry. Otherwise,
        the current time is used.

        Returns True if the entry is successfully written, False otherwise.
        """

    @abstractmethod
    def remove(self, name: str) -> None:
        """Delete a metadata entry"""

    @abstractmethod
    def commit(self) -> None:
        """If the backing store requires a commit, do it.

        But N.B. that this is not *guaranteed* to do anything, and
        there is no guarantee that changes are not made until it is
        called.
        """

    @abstractmethod
    def list_all(self) -> Iterable[str]:
        ...


def random_string() -> str:
    return binascii.hexlify(os.urandom(8)).decode("ascii")


class FilesystemMetadataStore(MetadataStore):
    def __init__(self, cache_dir_prefix: str) -> None:
        # We check startswith instead of equality because the version
        # will have already been appended by the time the cache dir is
        # passed here.
        if cache_dir_prefix.startswith(os.devnull):
            self.cache_dir_prefix = None
        else:
            self.cache_dir_prefix = cache_dir_prefix

    def getmtime(self, name: str) -> float:
        if not self.cache_dir_prefix:
            raise FileNotFoundError()

        return int(os.path.getmtime(os.path.join(self.cache_dir_prefix, name)))

    def read(self, name: str) -> str:
        assert os.path.normpath(name) != os.path.abspath(name), "Don't use absolute paths!"

        if not self.cache_dir_prefix:
            raise FileNotFoundError()

        with open(os.path.join(self.cache_dir_prefix, name)) as f:
            return f.read()

    def write(self, name: str, data: str, mtime: float | None = None) -> bool:
        assert os.path.normpath(name) != os.path.abspath(name), "Don't use absolute paths!"

        if not self.cache_dir_prefix:
            return False

        path = os.path.join(self.cache_dir_prefix, name)
        tmp_filename = path + "." + random_string()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(tmp_filename, "w") as f:
                f.write(data)
            os.replace(tmp_filename, path)
            if mtime is not None:
                os.utime(path, times=(mtime, mtime))

        except OSError:
            return False
        return True

    def remove(self, name: str) -> None:
        if not self.cache_dir_prefix:
            raise FileNotFoundError()

        os.remove(os.path.join(self.cache_dir_prefix, name))

    def commit(self) -> None:
        pass

    def list_all(self) -> Iterable[str]:
        if not self.cache_dir_prefix:
            return

        for dir, _, files in os.walk(self.cache_dir_prefix):
            dir = os.path.relpath(dir, self.cache_dir_prefix)
            for file in files:
                yield os.path.join(dir, file)


def connect_db(
    host: str, port: int, dbname: str, user: str, password: str
) -> psycopg2.extensions.connection:
    """Connect to a PostgreSQL database."""
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    return conn


class SqliteMetadataStore(MetadataStore):
    """A PostgreSQL backed implementation for metadata storage."""

    def __init__(self, cache_dir_prefix: str) -> None:
        self.db = connect_db("127.0.0.1", 5432, "mydatabase", "test", "test")
        self.initialize_schema()

    def initialize_schema(self):
        """Initialize the database schema."""
        with self.db.cursor() as cur:
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY NOT NULL,
                mtime REAL NOT NULL,
                data TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS path_idx ON files(path);
            """
            )
        self.db.commit()

    def getmtime(self, name: str) -> float:
        """Get the modification time of a metadata entry."""
        with self.db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT mtime FROM files WHERE path = %s", (name,))
            result = cur.fetchone()
            if not result:
                raise FileNotFoundError("No entry found for the specified name.")
            return result["mtime"]

    def read(self, name: str) -> str:
        """Read the data of a metadata entry."""
        with self.db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT data FROM files WHERE path = %s", (name,))
            result = cur.fetchone()
            if not result:
                raise FileNotFoundError("No entry found for the specified name.")
            return result["data"]

    def write(self, name: str, data: str, mtime: float | None = None) -> bool:
        """Write a metadata entry."""
        if mtime is None:
            mtime = time.time()
        with self.db.cursor() as cur:
            try:
                cur.execute(
                    "INSERT INTO files (path, mtime, data) VALUES (%s, %s, %s) ON CONFLICT (path) DO UPDATE SET mtime = EXCLUDED.mtime, data = EXCLUDED.data",
                    (name, mtime, data),
                )
            except psycopg2.Error:
                return False
            return True

    def remove(self, name: str) -> None:
        """Remove a metadata entry."""
        with self.db.cursor() as cur:
            cur.execute("DELETE FROM files WHERE path = %s", (name,))

    def commit(self) -> None:
        """Commit the transaction to the database."""
        self.db.commit()

    def list_all(self) -> Iterable[str]:
        """List all metadata entries."""
        with self.db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT path FROM files")
            for row in cur:
                yield row["path"]

