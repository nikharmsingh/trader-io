from typing import Optional

from fastapi import APIRouter, Query

from db.clickhouse import query
from models.schemas import AlertResponse, PaginatedResponse

router = APIRouter(prefix="/alerts", tags=["Alerts"])


@router.get("/history", response_model=PaginatedResponse)
def get_alert_history(
    symbol: Optional[str] = Query(None),
    alert_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    where_clauses = ["1=1"]
    if symbol:
        where_clauses.append(f"symbol = '{symbol.upper()}'")
    if alert_type:
        where_clauses.append(f"alert_type = '{alert_type}'")
    where = " AND ".join(where_clauses)

    total = query(f"SELECT count() FROM alerts WHERE {where}")[0][0]
    offset = (page - 1) * page_size

    rows = query(f"""
        SELECT toString(id), symbol, alert_type, message,
               price, indicator_value, triggered_at
        FROM alerts
        WHERE {where}
        ORDER BY triggered_at DESC
        LIMIT {page_size} OFFSET {offset}
    """)

    data = [
        AlertResponse(
            id=r[0], symbol=r[1], alert_type=r[2], message=r[3],
            price=r[4], indicator_value=r[5], triggered_at=r[6],
        )
        for r in rows
    ]
    return PaginatedResponse(
        data=[d.model_dump() for d in data],
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + page_size) < total,
    )
