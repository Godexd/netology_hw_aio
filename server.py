from functools import partial
from datetime import date
from aiohttp import web
from aiopg import pool
from aiopg.sa import create_engine
from sqlalchemy import (
    Column,  Integer, MetaData, String, Table
)
import config

metadata = MetaData()

ads_table = Table(
    'ads',
    metadata,
    Column('id', Integer, autoincrement=True,
           primary_key=True, nullable=False),
    Column('title', String, nullable=False),
    Column('description', String, nullable=False),
    Column('creator', String),
    Column('create_up', String)
)


class Ads(web.View):
    async def post(self):
        post_data = await self.request.json()
        try:
            title = post_data['title']
            description = post_data['description']
            creator = post_data['creator']
            day = date.today()
            day_str = str(day)
            create_up = day_str
        except KeyError:
            raise web.HTTPBadRequest
        engine = self.request.app['pg_engine']

        async with engine.acquire() as conn:
            result = await conn.execute(ads_table.insert().values(title=title, description=description,
                                                                  creator=creator, create_up=create_up))
            ads = await result.fetchone()
            return web.json_response({'ads_id': ads[0]})

    async def get(self):
        engine = self.request.app['pg_engine']
        async with engine.acquire() as conn:
            q_user = ads_table.select()
            ads = await (await conn.execute(q_user)).fetchall()
            list_ads = []
            for r in ads:
                ads_dict = {
                    'id': r[0],
                    'title': r[1],
                    'description': r[2],
                    'creator': r[3],
                    'create_up': r[4]
                }
                list_ads.append(ads_dict)
            return web.json_response(list_ads)


class Ads_one(web.View):

    async def get(self):
        ads_id = self.request.match_info['ads_id']
        engine = self.request.app['pg_engine']
        async with engine.acquire() as conn:
            where = ads_table.c.id == ads_id
            q_user = ads_table.select().where(where)
            ads = await (await conn.execute(q_user)).fetchone()
            if ads:
                return web.json_response({
                    'id': ads[0],
                    'title': ads[1],
                    'description': ads[2],
                    'creator': ads[3],
                    'create_up': ads[4]
                })
        raise web.HTTPNotFound()

    async def delete(self):
        ads_id = self.request.match_info['ads_id']
        engine = self.request.app['pg_engine']
        async with engine.acquire() as conn:
            where = ads_table.c.id == ads_id
            await conn.execute(ads_table.delete().where(where))
            return web.json_response({ads_id: "delete"})

    async def put(self):
        ads_id = self.request.match_info['ads_id']
        post_data = await self.request.json()
        engine = self.request.app['pg_engine']
        async with engine.acquire() as conn:
            where = ads_table.c.id == ads_id
            day = date.today()
            day_str = str(day)
            await conn.execute(ads_table.update().values({"title": post_data['title'],
                                                          "description": post_data['description'], "create_up": day_str}).where(where))
            q_user = ads_table.select().where(where)
            ads = await (await conn.execute(q_user)).fetchone()
            return web.json_response({
                'id': ads[0],
                'title': ads[1],
                'description': ads[2],
                'creator': ads[3],
                'create_up': ads[4]})



async def register_connection_alchemy(app: web.Application):
    engine = await create_engine(
        dsn=config.POSTGRE_DSN,
        minsize=2,
        maxsize=10)

    app['pg_engine'] = engine
    yield
    engine.close()


async def get_app():
    app = web.Application()
    app.cleanup_ctx.append(partial(register_connection_alchemy))
    app.router.add_view('/ads/', Ads)
    app.router.add_view('/ads/{ads_id:\d+}', Ads_one)
    return app

if __name__ == '__main__':
    app = get_app()
    web.run_app(app, host='127.0.0.1', port=8080)
