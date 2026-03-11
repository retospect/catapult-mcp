"""Shared fixtures for catapult tests."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from catapult.db.schema import Base, Reaction


@pytest.fixture
def engine():
    """In-memory SQLite engine with schema tables."""
    eng = create_engine(
        "sqlite:///:memory:",
        execution_options={"schema_translate_map": {"catapult": None}},
    )

    @event.listens_for(eng, "connect")
    def _set_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def session(engine):
    """Session with sample reaction data."""
    Session = sessionmaker(bind=engine)
    s = Session()

    reactions = [
        Reaction(
            equation="CO* -> CO2",
            catalyst="Pd",
            facet="111",
            reactants="CO",
            products="CO2",
            energy=-0.72,
            barrier=0.89,
            site="fcc",
            functional="BEEF-vdW",
            database="cathub",
            doi="10.1021/acscatal.7b02335",
            pub_id="MedfordEtAl2017",
            sys_id="rxn_001",
        ),
        Reaction(
            equation="CO* -> CO2",
            catalyst="Pd",
            facet="100",
            reactants="CO",
            products="CO2",
            energy=-0.58,
            barrier=1.02,
            site="bridge",
            functional="BEEF-vdW",
            database="cathub",
            doi="10.1021/acscatal.7b02335",
            pub_id="MedfordEtAl2017",
            sys_id="rxn_002",
        ),
        Reaction(
            equation="CO* -> CO2",
            catalyst="Pt",
            facet="111",
            reactants="CO",
            products="CO2",
            energy=-0.91,
            barrier=0.78,
            site="atop",
            functional="BEEF-vdW",
            database="cathub",
            doi="10.1021/acscatal.7b02335",
            pub_id="MedfordEtAl2017",
            sys_id="rxn_003",
        ),
        Reaction(
            equation="H2O* -> OH* + H*",
            catalyst="Cu",
            facet="111",
            reactants="H2O",
            products="OH,H",
            energy=-0.33,
            barrier=1.35,
            site="fcc",
            functional="PBE",
            database="cathub",
            doi="10.1021/ja5088237",
            pub_id="NorskovEtAl2014",
            sys_id="rxn_004",
        ),
        Reaction(
            equation="O2* -> 2O*",
            catalyst="Pt",
            facet="111",
            reactants="O2",
            products="O",
            energy=-1.45,
            barrier=0.52,
            site="bridge",
            functional="RPBE",
            database="cathub",
            sys_id="rxn_005",
        ),
    ]
    s.add_all(reactions)
    s.commit()
    yield s
    s.close()
