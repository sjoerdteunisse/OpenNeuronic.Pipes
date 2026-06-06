from __future__ import annotations

from openneuronic.pipes.core.bookmark import Bookmark
from openneuronic.pipes.core.enums import BookmarkType


def test_bookmark_construction() -> None:
    bm = Bookmark(
        pipe_id="orders-sync",
        column="updated_at",
        type=BookmarkType.DATETIME,
        value="2024-01-01T00:00:00",
    )
    assert bm.pipe_id == "orders-sync"
    assert bm.column == "updated_at"
    assert bm.type == BookmarkType.DATETIME
    assert bm.value == "2024-01-01T00:00:00"
    assert bm.previous_value is None
    assert bm.batch_count == 0


def test_bookmark_previous_value_tracks_rollback() -> None:
    bm = Bookmark(
        pipe_id="p",
        column="id",
        type=BookmarkType.INTEGER,
        value=100,
    )
    # Simulate the runner advancing the bookmark.
    bm.previous_value = bm.value
    bm.value = 200
    bm.batch_count += 1

    assert bm.value == 200
    assert bm.previous_value == 100
    assert bm.batch_count == 1

    # Simulate a rollback by restoring previous_value.
    bm.value = bm.previous_value
    assert bm.value == 100


def test_bookmark_all_types() -> None:
    for bt in BookmarkType:
        bm = Bookmark(pipe_id="p", column="c", type=bt, value=None)
        assert bm.type == bt
