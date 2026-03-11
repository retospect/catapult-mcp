"""catapult_get implementation — formats query results as markdown strings."""

from __future__ import annotations

from typing import Any

from chemdb.cite import format_citation
from chemdb.config import ChemdbConfig, load_config
from chemdb.db import make_engine, make_session
from chemdb.errors import ChemdbError

from catapult.db.query import PAGE_SIZE, get_by_id, get_shape, search
from catapult.db.schema import SCHEMA

_config: ChemdbConfig | None = None
_SessionFactory = None


def _get_session():
    global _config, _SessionFactory
    if _SessionFactory is None:
        _config = load_config()
        engine = make_engine(_config, SCHEMA)
        _SessionFactory = make_session(engine)
    return _SessionFactory()


def catapult_get(
    id: str = "",
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
) -> str:
    """Query heterogeneous catalysis databases.

    Returns formatted markdown with results, shape, and hints.
    """
    try:
        session = _get_session()

        # ID lookup mode
        if id:
            data = get_by_id(session, id)
            return _format_publication(data)

        # No args → shape of entire database
        has_filters = any(
            [
                query,
                catalyst,
                facet,
                reactants,
                products,
                energy,
                barrier,
                functional,
                database,
                sort,
            ]
        )
        if not has_filters and page == 1:
            data = get_shape(session)
            return _format_shape(data)

        # Filter search
        data = search(
            session,
            query=query,
            catalyst=catalyst,
            facet=facet,
            reactants=reactants,
            products=products,
            energy=energy,
            barrier=barrier,
            functional=functional,
            database=database,
            sort=sort,
            page=page,
        )
        return _format_search(data, catalyst=catalyst)

    except ChemdbError as exc:
        return exc.to_markdown()
    except Exception as exc:
        return f"⚠ Internal error: {exc}"


# ── Formatters ──────────────────────────────────────────────────────


def _format_publication(data: dict[str, Any]) -> str:
    """Format publication/ID lookup results."""
    total = data["total"]
    results = data["results"]

    lines = [f"## {total} reactions for {data['id']}"]
    lines.append("")

    for i, r in enumerate(results, 1):
        lines.append(_format_rxn_line(i, r))

    if total > PAGE_SIZE:
        lines.append("")
        lines.append(f'→ catapult_get(id="{data["id"]}", page=2) for next {PAGE_SIZE}')

    return "\n".join(lines)


def _format_shape(data: dict[str, Any]) -> str:
    """Format full database shape (no-arg call)."""
    total = data.get("total", 0)

    lines = [f"## CataPult — ~{total:,} reactions"]
    lines.append("")

    cats = data.get("catalysts", {})
    if cats:
        lines.append("### Top catalysts (by reaction count)")
        parts = [f"{cat} {count:,}" for cat, count in list(cats.items())[:8]]
        lines.append(" | ".join(parts))

    facets = data.get("facets", {})
    if facets:
        lines.append("")
        lines.append("### Facets")
        parts = [f"{f} {count:,}" for f, count in list(facets.items())[:8]]
        lines.append(" | ".join(parts))

    e = data.get("energy", {})
    b = data.get("barrier", {})
    if e.get("min") is not None:
        lines.append("")
        lines.append("### Energy ranges")
        lines.append("| Property | Min | Avg | Max | Unit |")
        lines.append("|----------|-----|-----|-----|------|")
        if e.get("min") is not None:
            lines.append(
                f"| ΔE (reaction) | {e['min']:.1f} | {e['avg']:.1f} | {e['max']:.1f} | eV |"
            )
        if b.get("min") is not None:
            lines.append(
                f"| Ea (barrier) | {b['min']:.1f} | {b['avg']:.1f} | {b['max']:.1f} | eV |"
            )

    funcs = data.get("functionals", {})
    if funcs:
        lines.append("")
        lines.append("### Functionals")
        parts = [f"{f} {count:,}" for f, count in list(funcs.items())[:6]]
        lines.append(" | ".join(parts))

    lines.append("")
    lines.append("### Sortable fields")
    lines.append("energy, barrier, catalyst, facet")
    lines.append("")
    lines.append('→ catapult_get(catalyst="Pd") to browse palladium reactions')
    lines.append(
        '→ catapult_get(reactants="CO", catalyst="Pd,Pt,Cu", facet="111") to compare metals'
    )
    lines.append('→ catapult_get(query="oxygen evolution") for free-text search')

    return "\n".join(lines)


def _format_search(data: dict[str, Any], catalyst: str = "") -> str:
    """Format search results with shape + hints."""
    total = data["total"]
    page = data["page"]
    start = (page - 1) * PAGE_SIZE + 1
    end = min(start + PAGE_SIZE - 1, total)
    comparison = data.get("comparison")

    # Comparison mode
    if comparison:
        return _format_comparison(data, catalyst)

    lines = [f"## {total} reactions (showing {start}–{end})"]
    lines.append("")

    # Shape on page 1
    shape = data.get("shape")
    if shape:
        lines.append("### Shape")
        e = shape.get("energy", {})
        b = shape.get("barrier", {})
        if e.get("min") is not None:
            lines.append(
                f"  ΔE  {e['min']:.1f} to {e['max']:.1f} eV  (mean {e['avg']:.2f})"
            )
        if b.get("min") is not None:
            lines.append(
                f"  Ea  {b['min']:.1f}–{b['max']:.1f} eV  (mean {b['avg']:.2f})"
            )
        facets = shape.get("facets", {})
        if facets:
            parts = [f"{f} ×{count}" for f, count in list(facets.items())[:5]]
            lines.append(f"  Facets: {' | '.join(parts)}")
        funcs = shape.get("functionals", {})
        if funcs:
            parts = [f"{f} {count}" for f, count in list(funcs.items())[:4]]
            lines.append(f"  Functionals: {' | '.join(parts)}")
        lines.append("")

    for i, r in enumerate(data["results"], start):
        lines.append(_format_rxn_line(i, r))

    # Hints
    lines.append("")
    if end < total:
        lines.append(f"→ catapult_get(..., page={page + 1}) for next {PAGE_SIZE}")
    if data["results"] and data["results"][0].get("doi"):
        doi = data["results"][0]["doi"]
        lines.append(f'→ catapult_get(id="doi:{doi}") for full pub details')

    return "\n".join(lines)


def _format_comparison(data: dict[str, Any], catalyst: str) -> str:
    """Format multi-catalyst comparison table."""
    comparison = data["comparison"]
    total = data["total"]

    lines = [f"## Comparison — {len(comparison)} catalysts ({total} total reactions)"]
    lines.append("")
    lines.append("| Surface | ΔE (eV) | Ea (eV) | Site | Functional | Source |")
    lines.append("|---------|---------|---------|------|------------|--------|")

    for r in comparison:
        cat_facet = r.get("catalyst", "?")
        if r.get("facet"):
            cat_facet += f"({r['facet']})"
        energy_s = f"{r['energy']:.2f}" if r.get("energy") is not None else "?"
        barrier_s = f"{r['barrier']:.2f}" if r.get("barrier") is not None else "?"
        cite = r.get("doi", "")
        if cite:
            cite = f"[@{cite}]"
        lines.append(
            f"| {cat_facet} | {energy_s} | {barrier_s} | {r.get('site', '')} | {r.get('functional', '')} | {cite} |"
        )

    lines.append("")
    cats = catalyst.split(",")
    if cats:
        lines.append(
            f'→ catapult_get(catalyst="{cats[0].strip()}") for all {cats[0].strip()} reactions'
        )

    return "\n".join(lines)


def _format_rxn_line(i: int, r: dict[str, Any]) -> str:
    """Format a single reaction result line."""
    cat_facet = r.get("catalyst", "?")
    if r.get("facet"):
        cat_facet += f"({r['facet']})"

    parts = [f"{cat_facet} — {r.get('equation', '?')}"]
    if r.get("energy") is not None:
        parts.append(f"ΔE {r['energy']:.2f} eV")
    if r.get("barrier") is not None:
        parts.append(f"Ea {r['barrier']:.2f} eV")
    if r.get("functional"):
        parts.append(r["functional"])

    line = f"{i:>2}. {parts[0]}    {'  |  '.join(parts[1:])}"

    if r.get("doi"):
        cite = format_citation(r["doi"])
        line += f"\n    {cite}"

    return line
