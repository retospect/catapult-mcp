# catapult-mcp

MCP server for querying heterogeneous catalysis databases. Exposes reaction data, catalysts, and conditions via the Model Context Protocol.

## Features

- **Reaction search** — query by catalyst, reactant, product, or conditions
- **SQLAlchemy backend** — SQLite (default) or PostgreSQL
- **MCP protocol** — compatible with any MCP-aware LLM client

## Installation

```bash
uv pip install -e .
# With PostgreSQL support:
uv pip install -e ".[postgres]"
```

## Usage

```bash
catapult-mcp   # starts the MCP server
```

## Dependencies

- **chemdb-common** — shared database models and CLI
- **mcp** — Model Context Protocol server framework

## License

GPL-3.0-or-later — see [LICENSE](LICENSE).
