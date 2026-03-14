from collections import deque


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
        self.handlers = {}
        self.executed_sql = []
        for needle, payload in handlers.items():
            if isinstance(payload, list):
                self.handlers[needle] = deque(payload)
            else:
                self.handlers[needle] = payload

    def execute(self, query, params):
        sql = str(query)
        self.executed_sql.append(sql)
        for needle, payload in self.handlers.items():
            if needle not in sql:
                continue

            if isinstance(payload, deque):
                if not payload:
                    raise AssertionError(f"No remaining payloads for query needle: {needle}")
                payload = payload.popleft()

            result = payload(params) if callable(payload) else payload
            return InMemoryResult(
                scalar=result.get("scalar"),
                rows=result.get("rows"),
            )

        raise AssertionError(f"Unexpected query in InMemoryDB: {sql}")

    def commit(self):
        return None

    def rollback(self):
        return None
