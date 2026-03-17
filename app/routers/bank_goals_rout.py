from fastapi import routing, Depends
from fastapi.responses import StreamingResponse
from fastapi import Response

from services.bank_goals_service import bankGoalsService
from dal.bank_goals_dal import MySqlDalBankGoals
from core.dependency import get_sql_conn

router = routing.APIRouter()

@router.get('/q1')
def get_shifting_quality_goals(conn = Depends(get_sql_conn)):
    return bankGoalsService(MySqlDalBankGoals, conn).get_shifting_quality_goals()


@router.get('/q2')
def get_analysis_collection_sources(conn = Depends(get_sql_conn)):
    return bankGoalsService(MySqlDalBankGoals, conn).get_analysis_collection_sources()


@router.get('/q3')
def get_optional_new_goals(conn = Depends(get_sql_conn)):
    return bankGoalsService(MySqlDalBankGoals, conn).get_optional_new_goals()


@router.get('/q4')
def identifying_awakened_sleeping_cells(conn = Depends(get_sql_conn)):
    return bankGoalsService(MySqlDalBankGoals, conn).identifying_awakened_sleeping_cells()


@router.get('/q5/{entity_id}')
def get_lan_lot_by_entity_id_order_time(entity_id: str, conn = Depends(get_sql_conn)):
    img_bit = bankGoalsService(MySqlDalBankGoals, conn).get_lan_lot_by_entity_id_order_time(entity_id)

    return Response(content=img_bit, media_type="image/png")


@router.get('/q6')
def get_lan_lot_by_entity_id_order_time(conn = Depends(get_sql_conn)):
    return bankGoalsService(MySqlDalBankGoals, conn).get_analyzing_escape_patterns_after_attack()


@router.get('/q7')
def finding_friends_meetings(conn = Depends(get_sql_conn)):
    return bankGoalsService(MySqlDalBankGoals, conn).finding_friends_meetings()

     

