class InMemoryResult:
    def __init__(self, scalar=None, rows=None):
        self.scalar = scalar
        self.rows = list(rows or [])

    def scalar_one(self):
        return self.scalar

    def scalar_one_or_none(self):
        return self.scalar

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return list(self.rows)

    def __iter__(self):
        return iter(self.rows)


class InMemoryDB:
    def __init__(self, handlers):
        self.handlers = handlers

    def execute(self, query, params):
        sql = str(query)
        for needle, payload in self.handlers.items():
            if needle not in sql:
                continue

            result = payload(params) if callable(payload) else payload
            return InMemoryResult(
                scalar=result.get("scalar"),
                rows=result.get("rows"),
            )

        raise AssertionError(f"Unexpected query in InMemoryDB: {sql}")
