"""README example tests — Bookmarks section."""
from __future__ import annotations

import datetime

import pytest

from openneuronic.pipes import (
    Bookmark,
    BookmarkType,
    InMemoryBookmarkStore,
)


async def test_bookmark_save_and_load() -> None:
    bm_int = Bookmark(
        pipe_id="p",
        column="sequence_id",
        type=BookmarkType.INTEGER,
        value=1000,
    )
    store = InMemoryBookmarkStore()
    await store.save(bm_int)

    loaded = await store.load("p")
    assert loaded is not None
    assert loaded.value == 1000


async def test_bookmark_delete_removes_entry() -> None:
    bm = Bookmark(pipe_id="p2", column="id", type=BookmarkType.INTEGER, value=42)
    store = InMemoryBookmarkStore()
    await store.save(bm)
    await store.delete("p2")
    assert await store.load("p2") is None


async def test_bookmark_load_returns_none_when_absent() -> None:
    store = InMemoryBookmarkStore()
    assert await store.load("nonexistent") is None


def test_bookmark_types_construct() -> None:
    now = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    bm_dt  = Bookmark(pipe_id="p", column="updated_at",  type=BookmarkType.DATETIME,   value=now)
    bm_int = Bookmark(pipe_id="p", column="sequence_id", type=BookmarkType.INTEGER,    value=1000)
    bm_rv  = Bookmark(pipe_id="p", column="rowversion",  type=BookmarkType.ROWVERSION, value=b"\x00\x00\x00\x00\x00\x00\x00\x01")
    bm_lsn = Bookmark(pipe_id="p", column="lsn",         type=BookmarkType.LSN,        value="0/1AF0000")
    bm_cur = Bookmark(pipe_id="p", column="cursor",      type=BookmarkType.CURSOR,     value="opaque-string")

    assert bm_dt.type == BookmarkType.DATETIME
    assert bm_int.type == BookmarkType.INTEGER
    assert bm_rv.type == BookmarkType.ROWVERSION
    assert bm_lsn.type == BookmarkType.LSN
    assert bm_cur.type == BookmarkType.CURSOR


def test_bookmark_previous_value_stores_rollback_target() -> None:
    bookmark = Bookmark(
        pipe_id="p",
        column="id",
        type=BookmarkType.INTEGER,
        value=5000,
        previous_value=4500,
    )
    assert bookmark.previous_value == 4500


def test_bookmark_value_attribute_matches_given() -> None:
    bm = Bookmark(pipe_id="x", column="seq", type=BookmarkType.INTEGER, value=999)
    assert bm.value == 999


def test_bookmark_pipe_id_stored() -> None:
    bm = Bookmark(pipe_id="orders-sync", column="seq", type=BookmarkType.INTEGER, value=1)
    assert bm.pipe_id == "orders-sync"


async def test_bookmark_save_overwrites_existing() -> None:
    store = InMemoryBookmarkStore()
    bm1 = Bookmark(pipe_id="p", column="seq", type=BookmarkType.INTEGER, value=100)
    bm2 = Bookmark(pipe_id="p", column="seq", type=BookmarkType.INTEGER, value=200)
    await store.save(bm1)
    await store.save(bm2)
    loaded = await store.load("p")
    assert loaded.value == 200
