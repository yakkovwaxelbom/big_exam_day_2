from fastapi import FastAPI
import uvicorn
from contextlib import asynccontextmanager

from core.connections import get_sql_connection
from core.configs import MySqlConfig

from dal.bank_goals_dal import MySqlDalBankGoals

from routers.bank_goals_rout import router

@asynccontextmanager
async def lifespan(app: FastAPI):

    mysql_config = MySqlConfig().to_dict()
    app.state.mysql_pool = get_sql_connection(mysql_config)

    yield
    

app = FastAPI(lifespan=lifespan)
app.include_router(router)

@app.get('/test')
def test():
    return {'status': 'ok'}


if __name__ == '__main__':
    uvicorn.run('main:app', host='127.0.0.1', port=8000, reload=True)