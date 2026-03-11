"""Tests for catapult.tool — formatted output."""

from __future__ import annotations

from catapult.db.query import get_by_id, get_shape, search
from catapult.tool import _format_publication, _format_search, _format_shape


class TestFormatPublication:
    def test_pub_output(self, session):
        data = get_by_id(session, "doi:10.1021/acscatal.7b02335")
        text = _format_publication(data)
        assert "reactions" in text
        assert "CO" in text


class TestFormatShape:
    def test_shape_output(self, session):
        data = get_shape(session)
        text = _format_shape(data)
        assert "CataPult" in text
        assert "Top catalysts" in text
        assert "Sortable fields" in text
        assert "→ get" in text


class TestFormatSearch:
    def test_search_output(self, session):
        data = search(session, catalyst="Pd")
        text = _format_search(data, catalyst="Pd")
        assert "reactions" in text
        assert "Shape" in text

    def test_comparison_output(self, session):
        data = search(session, catalyst="Pd,Pt", facet="111")
        text = _format_search(data, catalyst="Pd,Pt")
        assert "Comparison" in text
        assert "Surface" in text
