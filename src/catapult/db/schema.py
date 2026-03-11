"""SQLAlchemy models for local catalysis reaction data."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase

SCHEMA = "catapult"


class Base(DeclarativeBase):
    pass


class Reaction(Base):
    """A DFT-computed catalytic reaction."""

    __tablename__ = "reactions"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    equation = Column(Text, nullable=False)
    catalyst = Column(String(128), nullable=False, index=True)
    facet = Column(String(64), nullable=True, index=True)
    reactants = Column(Text, nullable=True)  # comma-separated
    products = Column(Text, nullable=True)  # comma-separated
    energy = Column(Float, nullable=True)  # reaction energy ΔE, eV
    barrier = Column(Float, nullable=True)  # activation barrier Ea, eV
    site = Column(Text, nullable=True)
    functional = Column(String(64), nullable=True)
    dft_code = Column(String(64), nullable=True)
    database = Column(String(32), nullable=False)  # cathub, mp
    doi = Column(Text, nullable=True)
    pub_id = Column(String(128), nullable=True, index=True)
    sys_id = Column(String(128), nullable=True)


class SyncLog(Base):
    """Sync history."""

    __tablename__ = "sync_log"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    row_count = Column(Integer, nullable=False, default=0)
    duration_s = Column(Float, nullable=True)


# Indexes for common queries
Index("idx_reactions_energy", Reaction.energy)
Index("idx_reactions_barrier", Reaction.barrier)
Index("idx_reactions_functional", Reaction.functional)
Index("idx_reactions_database", Reaction.database)
