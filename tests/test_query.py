"""Tests for catapult.db.query."""

from __future__ import annotations

import pytest
from chemdb.errors import IdNotFoundError, NoResultsError

from catapult.db.query import get_by_id, get_shape, search


class TestGetById:
    def test_doi_prefix(self, session):
        result = get_by_id(session, "doi:10.1021/acscatal.7b02335")
        assert result["type"] == "publication"
        assert result["total"] == 3  # 3 reactions from Medford

    def test_pub_prefix(self, session):
        result = get_by_id(session, "pub:MedfordEtAl2017")
        assert result["total"] == 3

    def test_sys_prefix(self, session):
        result = get_by_id(session, "sys:rxn_001")
        assert result["total"] == 1

    def test_bare_string_as_pub(self, session):
        result = get_by_id(session, "MedfordEtAl2017")
        assert result["total"] == 3

    def test_not_found(self, session):
        with pytest.raises(IdNotFoundError):
            get_by_id(session, "doi:10.9999/nonexistent")


class TestSearch:
    def test_catalyst_single(self, session):
        result = search(session, catalyst="Pd")
        assert result["total"] == 2

    def test_catalyst_multi_comparison(self, session):
        result = search(session, catalyst="Pd,Pt")
        assert result["total"] == 4  # 2 Pd + 2 Pt
        assert result["comparison"] is not None

    def test_facet_filter(self, session):
        result = search(session, facet="111")
        assert result["total"] == 4

    def test_reactants_filter(self, session):
        result = search(session, reactants="CO")
        assert result["total"] == 3

    def test_energy_range(self, session):
        result = search(session, energy="-1..0")
        assert result["total"] == 4  # all except O2 dissociation at -1.45

    def test_barrier_range(self, session):
        result = search(session, barrier="<1.0")
        assert result["total"] == 3  # Pd(111) 0.89, Pt(111) 0.78, Pt(111) 0.52

    def test_functional_filter(self, session):
        result = search(session, functional="PBE")
        assert result["total"] == 1  # Cu only

    def test_database_filter(self, session):
        result = search(session, database="cathub")
        assert result["total"] == 5

    def test_no_results(self, session):
        with pytest.raises(NoResultsError):
            search(session, catalyst="Au")

    def test_shape_on_page_1(self, session):
        result = search(session, catalyst="Pd")
        assert result["shape"] is not None
        assert "energy" in result["shape"]


class TestGetShape:
    def test_full_shape(self, session):
        shape = get_shape(session)
        assert shape["total"] == 5
        assert "catalysts" in shape
        assert "facets" in shape
        assert "energy" in shape
