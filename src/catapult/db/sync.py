"""Sync upstream catalysis databases to local storage.

Fetches data from Catalysis-Hub GraphQL and (optionally) Materials Project,
then upserts into the local DB (Postgres schema or SQLite file).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from chemdb.config import ChemdbConfig
from chemdb.db import ensure_schema, make_engine, make_session
from catapult.db.schema import SCHEMA, Base, Reaction, SyncLog

log = logging.getLogger(__name__)

CATHUB_GRAPHQL = "https://api.catalysis-hub.org/graphql"
CATHUB_PAGE_SIZE = 200


class CatapultSyncer:
    """Downloads catalysis data from upstream sources into local DB."""

    def __init__(self, config: ChemdbConfig):
        self.config = config
        self.engine = make_engine(config, SCHEMA)
        self.Session = make_session(self.engine)

    def run(self, *, force: bool = False):
        """Run the full sync."""
        ensure_schema(self.engine, SCHEMA)

        if force:
            Base.metadata.drop_all(self.engine)

        Base.metadata.create_all(self.engine)

        t0 = time.time()

        count = self._sync_cathub()
        if self.config.mp_api_key:
            count += self._sync_mp()

        duration = time.time() - t0
        self._log_sync("all", count, duration)
        log.info("Sync complete: %d rows in %.1fs", count, duration)

    def _sync_cathub(self) -> int:
        """Fetch reactions from Catalysis-Hub GraphQL API.

        Uses keyset pagination: id > last_id, ordered by id.
        """
        import base64
        import httpx

        log.info("Syncing from Catalysis-Hub...")
        count = 0
        last_id = 0

        with self.Session() as session:
            while True:
                query = _cathub_query(last_id, CATHUB_PAGE_SIZE)
                try:
                    resp = httpx.post(
                        CATHUB_GRAPHQL,
                        json={"query": query},
                        timeout=60,
                    )
                    resp.raise_for_status()
                except Exception as exc:
                    log.error("Catalysis-Hub fetch failed: %s", exc)
                    break

                data = resp.json()
                reactions = data.get("data", {}).get("reactions")
                if reactions is None:
                    errors = data.get("errors", [])
                    log.error("GraphQL error: %s", errors)
                    break

                edges = reactions.get("edges", [])
                if not edges:
                    break

                for edge in edges:
                    node = edge.get("node", {})
                    # Extract numeric id from base64 "Reaction:NNN"
                    raw_id = node.get("id", "")
                    try:
                        decoded = base64.b64decode(raw_id).decode()
                        last_id = int(decoded.split(":", 1)[1])
                    except Exception:
                        pass

                    rxn = self._cathub_to_model(node)
                    if rxn:
                        session.add(rxn)
                        count += 1

                session.flush()
                if count % 5000 < CATHUB_PAGE_SIZE:
                    session.commit()
                    log.info("  %d reactions (last_id=%d)...", count, last_id)

                if len(edges) < CATHUB_PAGE_SIZE:
                    break

            session.commit()

        return count

    def _sync_mp(self) -> int:
        """Fetch bulk properties from Materials Project."""
        log.info("MP sync: not yet implemented (needs mp-api client)")
        return 0

    def _cathub_to_model(self, node: dict) -> Reaction | None:
        """Convert a Catalysis-Hub GraphQL node to a Reaction model."""
        equation = node.get("Equation", "")
        if not equation:
            return None

        # Parse reactants/products from equation
        reactants = ""
        products = ""
        if " -> " in equation:
            parts = equation.split(" -> ", 1)
            reactants = parts[0].strip()
            products = parts[1].strip()

        # Prefer JSON reactants/products fields if available, else parse equation
        raw_reactants = node.get("reactants") or ""
        raw_products = node.get("products") or ""
        if raw_reactants and isinstance(raw_reactants, str) and raw_reactants != "{}":
            reactants = raw_reactants
        if raw_products and isinstance(raw_products, str) and raw_products != "{}":
            products = raw_products

        return Reaction(
            equation=equation,
            catalyst=node.get("chemicalComposition", ""),
            facet=node.get("facet", ""),
            reactants=reactants,
            products=products,
            energy=_float(node.get("reactionEnergy")),
            barrier=_float(node.get("activationEnergy")),
            site=node.get("sites", ""),
            functional=node.get("dftFunctional", ""),
            dft_code=node.get("dftCode", ""),
            database="cathub",
            pub_id=node.get("pubId"),
        )

    def _log_sync(self, source: str, count: int, duration: float):
        with self.Session() as session:
            session.add(
                SyncLog(
                    source=source,
                    synced_at=datetime.utcnow(),
                    row_count=count,
                    duration_s=duration,
                )
            )
            session.commit()


def _cathub_query(last_id: int, page_size: int) -> str:
    """Build a Catalysis-Hub GraphQL query with keyset pagination."""
    id_filter = f', id: {last_id}, op: ">"' if last_id > 0 else ""
    return f"""{{
  reactions(first: {page_size}, order: "id"{id_filter}) {{
    totalCount
    edges {{
      node {{
        id
        Equation
        reactionEnergy
        activationEnergy
        chemicalComposition
        surfaceComposition
        facet
        sites
        reactants
        products
        pubId
        dftCode
        dftFunctional
      }}
    }}
  }}
}}"""


def _float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
