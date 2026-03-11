"""Read-only queries for the get tool.

All queries are local SQL — no runtime API calls.
Shape is computed on-the-fly via SQL aggregation.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from chemdb.errors import IdNotFoundError, InvalidRangeError, NoResultsError
from chemdb.ranges import parse_range
from chemdb.sort import parse_sort

from catapult.db.schema import Reaction

PAGE_SIZE = 10

SORTABLE_FIELDS = {"energy", "barrier", "catalyst", "facet", "relevance"}


def get_by_id(session: Session, id_str: str) -> dict[str, Any]:
    """Look up reactions by identifier (doi:, pub:, sys:, or bare string)."""
    scheme, _, ident = id_str.partition(":")
    if not ident:
        # Bare string → publication ID search
        ident = scheme
        scheme = "pub"

    q = session.query(Reaction)
    if scheme == "doi":
        q = q.filter(Reaction.doi == ident)
    elif scheme == "pub":
        q = q.filter(Reaction.pub_id.ilike(f"%{ident}%"))
    elif scheme == "sys":
        q = q.filter(Reaction.sys_id == ident)
    else:
        raise IdNotFoundError(id_str)

    results = q.all()
    if not results:
        raise IdNotFoundError(id_str)

    return {
        "type": "publication",
        "id": id_str,
        "total": len(results),
        "results": [_rxn_to_row(r) for r in results[:PAGE_SIZE]],
    }


def search(
    session: Session,
    *,
    query: str = "",
    catalyst: str = "",
    facet: str = "",
    reactants: str = "",
    products: str = "",
    energy: str = "",
    barrier: str = "",
    functional: str = "",
    database: str = "",
    sort: str = "",
    page: int = 1,
) -> dict[str, Any]:
    """Filter search with shape + paginated results."""
    q = session.query(Reaction)

    # Catalyst filter (OR logic for comparison)
    catalysts = []
    if catalyst:
        catalysts = [c.strip() for c in catalyst.split(",") if c.strip()]
        if len(catalysts) == 1:
            q = q.filter(Reaction.catalyst.ilike(f"%{catalysts[0]}%"))
        else:
            from sqlalchemy import or_

            q = q.filter(or_(*[Reaction.catalyst.ilike(f"%{c}%") for c in catalysts]))

    if facet:
        q = q.filter(Reaction.facet == facet)

    if reactants:
        q = q.filter(Reaction.reactants.ilike(f"%{reactants}%"))

    if products:
        q = q.filter(Reaction.products.ilike(f"%{products}%"))

    # Range filters
    for field_name, raw in [("energy", energy), ("barrier", barrier)]:
        if raw:
            try:
                r = parse_range(raw)
            except ValueError:
                raise InvalidRangeError(field_name, raw)
            clause, params = r.to_sql_clause(field_name)
            q = q.filter(text(clause).bindparams(**params))

    if functional:
        q = q.filter(Reaction.functional.ilike(functional))

    if database:
        q = q.filter(Reaction.database.ilike(database))

    total = q.count()
    if total == 0:
        raise NoResultsError()

    # Comparison mode: multiple catalysts
    is_comparison = len(catalysts) > 1

    # Sort
    sort_fields = parse_sort(sort, SORTABLE_FIELDS) if sort else []
    if sort_fields:
        for sf in sort_fields:
            if sf.name == "relevance":
                continue
            col = getattr(Reaction, sf.name, None)
            if col is not None:
                q = q.order_by(col.desc() if sf.descending else col.asc())
    else:
        q = q.order_by(Reaction.energy.asc())

    offset = (page - 1) * PAGE_SIZE
    results = q.offset(offset).limit(PAGE_SIZE).all()

    shape = _compute_shape(session, q) if page == 1 else None

    # For comparison mode, also compute per-catalyst best
    comparison = None
    if is_comparison and page == 1:
        comparison = _compute_comparison(session, q, catalysts)

    return {
        "total": total,
        "page": page,
        "page_size": PAGE_SIZE,
        "shape": shape,
        "comparison": comparison,
        "results": [_rxn_to_row(r) for r in results],
    }


def get_shape(session: Session) -> dict[str, Any]:
    """Shape of entire database (no-arg call)."""
    total = session.query(Reaction).count()
    if total == 0:
        return {"total": 0}

    shape = _compute_shape(session, session.query(Reaction))
    shape["total"] = total
    return shape


def _compute_shape(session: Session, q) -> dict[str, Any]:
    """Compute shape statistics from a query."""
    stats = session.query(
        func.count(Reaction.id),
        func.min(Reaction.energy),
        func.max(Reaction.energy),
        func.avg(Reaction.energy),
        func.min(Reaction.barrier),
        func.max(Reaction.barrier),
        func.avg(Reaction.barrier),
    ).one()

    # Top catalysts
    cat_counts = (
        session.query(Reaction.catalyst, func.count(Reaction.id))
        .group_by(Reaction.catalyst)
        .order_by(func.count(Reaction.id).desc())
        .limit(10)
        .all()
    )

    # Top facets
    facet_counts = (
        session.query(Reaction.facet, func.count(Reaction.id))
        .filter(Reaction.facet.isnot(None), Reaction.facet != "")
        .group_by(Reaction.facet)
        .order_by(func.count(Reaction.id).desc())
        .limit(10)
        .all()
    )

    # Functionals
    func_counts = (
        session.query(Reaction.functional, func.count(Reaction.id))
        .filter(Reaction.functional.isnot(None), Reaction.functional != "")
        .group_by(Reaction.functional)
        .order_by(func.count(Reaction.id).desc())
        .limit(10)
        .all()
    )

    return {
        "energy": {"min": stats[1], "max": stats[2], "avg": stats[3]},
        "barrier": {"min": stats[4], "max": stats[5], "avg": stats[6]},
        "catalysts": {cat: count for cat, count in cat_counts},
        "facets": {f: count for f, count in facet_counts},
        "functionals": {f: count for f, count in func_counts},
    }


def _compute_comparison(
    session: Session, q, catalysts: list[str]
) -> list[dict[str, Any]]:
    """Best (lowest energy) row per catalyst for comparison table."""
    rows = []
    for cat in catalysts:
        best = (
            q.filter(Reaction.catalyst.ilike(f"%{cat}%"))
            .order_by(Reaction.energy.asc())
            .first()
        )
        if best:
            rows.append(_rxn_to_row(best))
    return rows


def _rxn_to_row(r: Reaction) -> dict[str, Any]:
    """Summary row for a reaction."""
    return {
        "equation": r.equation,
        "catalyst": r.catalyst,
        "facet": r.facet,
        "energy": r.energy,
        "barrier": r.barrier,
        "site": r.site,
        "functional": r.functional,
        "database": r.database,
        "doi": r.doi,
        "pub_id": r.pub_id,
    }
