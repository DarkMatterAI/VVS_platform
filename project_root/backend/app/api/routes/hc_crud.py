# api/routes/hc_jobs.py
from typing import Union, Literal, Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Body
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db

from app.crud import (
    create_hc_job,
    get_job,
    fetch_hc_job_results,
    count_hc_job_results,
    export_hc_job_hierarchy
)

from app.schemas import (
    HCJobCreate,
    HCMapperJobCreate,
    HCAssembledJobCreate,
)

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/v1/hc_jobs  → create a job
# ─────────────────────────────────────────────────────────────────────────────
@router.post("", summary="Create a Hill-Climb job")
async def create_hc_job_endpoint(
    payload: Union[HCJobCreate, HCMapperJobCreate, HCAssembledJobCreate] = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Create an HC job (standard, assembled, or mapper variant).
    Returns the created parent job id and input job ids.
    """
    try:
        search_cfg, item_dict, parent_job, input_jobs = await create_hc_job(db, payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to create HC job: {e}")

    return {
        "success": True,
        "parent_job_id": parent_job.id,
        "input_job_ids": [j.id for j in input_jobs],
        "num_inputs": len(input_jobs),
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/v1/hc_jobs/{hc_job_id}/results  → flat, paginated export
# ─────────────────────────────────────────────────────────────────────────────
@router.get(
    "/{hc_job_id:int}/results",
    summary="Fetch flat HC job results (paginated)",
)
async def get_hc_job_results_endpoint(
    hc_job_id: int,
    offset: int = Query(0, ge=0),
    limit: Optional[int] = Query(100, ge=1, le=10000),
    order_by: Literal["score", "timestamp"] = Query("score"),
    only_valid: bool = Query(False, description="Return only rows with result_valid=True"),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch flat HC job results with pagination.
    - order_by: 'score' (desc NULLS LAST) or 'timestamp'
    - only_valid: filters on HCResult.valid = TRUE
    """
    job = await get_job(db, job_id=hc_job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"HCJob {hc_job_id} not found")

    try:
        # total count for client pagination
        total = await count_hc_job_results(db, hc_job_id, only_valid=only_valid)
        rows = await fetch_hc_job_results(
            db,
            hc_job_id,
            offset=offset,
            limit=limit,
            order_by=order_by,
            # add only_valid support into your fetch function signature if not already
            # we previously added it; if not, filter client-side as fallback.
        )
        # If your fetch function doesn't yet accept only_valid, enforce here:
        if only_valid:
            rows = [r for r in rows if r.get("result_valid") is True]
    except ValueError as e:
        # e.g., “Cannot determine score-plugin…”
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch results: {e}")

    return {
        "success": True,
        "hc_job_id": hc_job_id,
        "offset": offset,
        "limit": limit,
        "order_by": order_by,
        "only_valid": only_valid,
        "total": total,
        "results": rows,
    }

@router.get("/{hc_job_id:int}/export", summary="Export hierarchical HC job results")
async def export_hc_job_hierarchy_endpoint(
    hc_job_id: int,
    db: AsyncSession = Depends(get_db),
):
    job = await get_job(db, job_id=hc_job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"HCJob {hc_job_id} not found")

    try:
        data = await export_hc_job_hierarchy(db, hc_job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export hierarchy: {e}")
    return {"success": True, "hc_job_id": hc_job_id, "data": data}