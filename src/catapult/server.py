"""MCP server for CataPult — one tool: get.

Queries heterogeneous catalysis databases (Catalysis-Hub, Materials Project).
All data is local (Postgres or SQLite). Run ``chemdb sync catapult`` first.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from catapult import tool

mcp = FastMCP("catapult")


@mcp.tool()
def get(
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

    id: identifier — "doi:10.1021/...", "pub:xyz", "sys:abc"
    query: FTS5 full-text search (always AND with filters)
    catalyst: composition, OR/comparison logic ("Pd", "Pd,Pt,Cu" = compare)
    facet: Miller index ("111", "100")
    reactants: reactant formula ("CO", "H2O")
    products: product formula
    energy: reaction energy range, eV ("-1.5..0", "<-0.5")
    barrier: activation barrier range, eV ("<1.0")
    functional: DFT method filter ("BEEF-vdW", "PBE")
    database: source filter ("cathub", "mp")
    sort: sort order ("energy", "!barrier", "energy,catalyst")
    page: page number (10 results/page)

    No args → shape of entire database (catalyst counts, energy ranges, top facets).
    id provided → all reactions from that publication.
    Any filter → search with results + shape on page 1.
    Multi-catalyst → comparison table (best per catalyst).
    """
    return tool.get(
        id=id,
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


def main():
    """Run the MCP server (stdio transport)."""
    mcp.run()


if __name__ == "__main__":
    main()
