"""In-memory ports for pipeline/runner tests."""
from prefect_flows.collection.enums import RunStatus


class FakeStore:
    def __init__(self, fail_on=None):
        self.objects, self.fail_on = {}, fail_on
    def put(self, path, key, content_type):
        if self.fail_on and self.fail_on(key):
            raise OSError("minio down")
        self.objects[key] = open(path, "rb").read()


class FakeRepo:
    """Runs + Bronze + known-URL ports."""
    def __init__(self, insert_error=False, race=False, known_urls=()):
        self.docs, self.checksums, self.known = [], set(), set(known_urls)
        self.runs, self.status_history = {}, []
        self.insert_error, self.race = insert_error, race
    def ensure_run(self, run_id, mode, source_name, params, params_version, dry_run, prefect_flow_run_id=None):
        self.runs[run_id] = dict(mode=mode, source_name=source_name, params=params, status=RunStatus.QUEUED)
    def mark_running(self, run_id):
        self.runs[run_id]["status"] = RunStatus.RUNNING
    def finalize_run(self, run_id, status, error_message=None, listing_pages_visited=0):
        self.runs[run_id].update(status=status, error=error_message)
    def checksum_exists(self, checksum):
        return checksum in self.checksums
    def insert_document(self, **kw):
        if self.insert_error:
            raise RuntimeError("db down")
        if self.race:
            return None
        self.checksums.add(kw["checksum"]); self.docs.append(kw)
        return len(self.docs)
    def known_url(self, canonical_url, source_type=None):
        return canonical_url in self.known
