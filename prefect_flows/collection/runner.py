"""Source-agnostic run loop, separated from Prefect, testable with fake ports."""
from typing import Optional

from prefect_flows.collection import runs
from prefect_flows.collection.deps import Deps
from prefect_flows.collection.pipeline import ingest_candidate
from prefect_flows.sources import registry


def run_collection(run_id: str, source_name: str, params: dict, deps: Deps, dry_run: bool = False,
                   params_version: int = 1, prefect_flow_run_id: Optional[str] = None, logger=None) -> dict:
    spec = registry.get(source_name)
    deps.repo.ensure_run(run_id, spec.mode, spec.name.value, params, params_version, dry_run, prefect_flow_run_id)

    with runs.track(deps.repo, run_id, deps.recorder):
        validated = registry.validate_params(spec.name, params)       # a bad payload fails the run visibly
        with deps.work:
            source = spec.build(validated, deps)
            user_provided = getattr(validated, "user_provided", None)
            for candidate in source.candidates():                       # streamed, one at a time
                if user_provided:                                       # researcher-typed metadata, honored by the extractors
                    candidate.raw_metadata = {**(candidate.raw_metadata or {}), "user_provided": user_provided}
                status = ingest_candidate(
                    candidate, source_name=spec.name.value, run_id=run_id, work=deps.work, fetcher=deps.fetcher,
                    store=deps.store, bronze=deps.repo, recorder=deps.recorder, dry_run=dry_run)
                if logger:
                    logger.info(f"{status.value}: {candidate.source_url}")

    summary = deps.recorder.summary()
    if logger:
        logger.info(f"Run {run_id} finished: {summary}")
    return summary
