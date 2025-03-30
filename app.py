from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from urllib.parse import quote_plus
from dataclasses import asdict
from random import sample
from datetime import timedelta, datetime
import os
import email.utils

from fastapi import FastAPI, Request, Query, BackgroundTasks
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.coder import PickleCoder
from sqlmodel import create_engine, SQLModel, Session
from asyncer import asyncify
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from utils import BadRequestResponseError, fetch, repeated_query_attempts
from curator import parse_met_art_object, parse_nasa_image, InvalidObject
from asteroid_astronomer import parse_asteroid_request
from post_management import Post, setup_search_table, PostRenderer, get_all_posts, search_posts, get_single_post
from cloudinary_images import get_recent_sunset_gif
from cme_astronomer import CoronalMassEjection, get_cme_data, update_cmes_with_lookback, build_cme_table
from exoplanet_astronomer import ExoPlanet, get_all_system_names, get_system, update_exoplanets_with_min_date, post_new_exoplanet_system

###################
# Startup Section #
###################


# This sets up cacheing
@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    FastAPICache.init(InMemoryBackend())
    scheduler = AsyncIOScheduler()
    scheduler.add_job(post_new_exoplanet_system, trigger="interval", hours=6, kwargs={'engine': engine})
    scheduler.start()
    
    repeated_query_attempts()(SQLModel.metadata.create_all)(engine)
    with Session(engine) as session:
        repeated_query_attempts()(setup_search_table)(session=session)
    yield

app = FastAPI(lifespan=lifespan)
internal_api = FastAPI()


app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/internal", internal_api)
templates = Jinja2Templates(directory='templates')
post_renderer = PostRenderer(
    post_template_path='templates/post.jinja.html',
    environment=templates.env
)

if conn_str := os.environ.get('SQLITECLOUD_CONNECTION'):
    engine = create_engine(conn_str, echo=False)
else:
    raise Exception('No connection string')

#########################
# HTML Endpoint Section #
#########################

@app.get('/')
async def root(request: Request, post_name: str|None = None) -> HTMLResponse:
    
    posts = await all_posts()
    
    return templates.TemplateResponse("post_index.html",
                                      {'request': request,
                                       'posts': posts})
    
#TODO - cache this
@app.get('/posts/{post_name}')
async def post_page(request: Request, post_name: str|None = None):
    if post_name:
        post = await asyncify(repeated_query_attempts()(get_single_post))(engine=engine, immutable_title=post_name)
        if post:
            rendered_post = post_renderer.render_post(post=post, request=request)
            return HTMLResponse(rendered_post)

    return RedirectResponse('/')

@app.get('/terminal')
async def terminal(request: Request, query: str | None = Query(None, max_length=200)):
    posts = await search(query=query) if query else []
    
    return templates.TemplateResponse("terminal_output.jinja.html",
                                      {'request': request,
                                       'results': posts})
    
@app.get('/random_art_html')
async def random_art_html(request: Request, art_type:str|None = Query(None, max_length=200, regex="^[a-z]+$")):
    object = await random_met_object(art_type=art_type)
    
    # This just makes pywright happy
    if type(object) is not dict:
        object = {}

    return templates.TemplateResponse("art.jinja.html",
                                      {'request': request,
                                       **object})
    
@app.get('/random_nasa_html')
async def random_nasa_html(request: Request, search_term:str|None = Query(None, max_length=200, regex="^[a-z]+$")):
    object = await random_nasa_image(search_term=search_term)
    
    # This just makes pywright happy
    if type(object) is not dict:
        object = {}
    
    return templates.TemplateResponse("art.jinja.html",
                                      {'request': request,
                                       **object})

###############
# RSS Section #
###############

@app.get('/rss')
async def rss(request: Request):

    posts = await all_posts()
    
    if type(posts) is list:
        post_rfc_822_dates = [email.utils.format_datetime(post.posted_at) for post in posts if post.posted_at]
    else:
        post_rfc_822_dates = []
    
    return templates.TemplateResponse("rss.xml",
                                      {'request': request,
                                       'posts': posts,
                                       'post_rfc_822_dates': post_rfc_822_dates,
                                       'site': dict(name='SullivanKelly dot com',
                                                    description='My blog',
                                                    url='https://www.sullivankelly.com')},
                                      media_type="application/xml"
    )
    
##########################
# Internal Api Endpoints #
##########################

## Posts ##

@internal_api.get("/all_posts", response_model=list[Post])
@cache(expire=60*5, coder=PickleCoder)  # 5 minutes
async def all_posts() -> list[Post]:
    posts = await asyncify(repeated_query_attempts()(get_all_posts))(engine=engine)
    return posts
    
## Met Art ##

@internal_api.get('/met_object_search')
@cache(expire=60*60*24)
async def met_object_search(art_type: str = Query(None, max_length=200, regex="^[a-z]+$")) -> list[int]:
    uri = f"https://collectionapi.metmuseum.org/public/collection/v1/search?hasImages=true&q={quote_plus(art_type)}"
    results = await fetch(uri)
    if type(results) is not dict:
        raise BadRequestResponseError(f"Expected dict, got {results}")
        
    return results.get('objectIDs', [])

@internal_api.get('/met_object')
@cache(expire=60*60*24)
async def met_object(object_id: int) -> dict[str, Any]:
    uri = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{object_id}"
    results = await fetch(uri)
    if type(results) is not dict:
        raise BadRequestResponseError(f"Expected dict, got {results}")
    
    return asdict(parse_met_art_object(results))
    
@internal_api.get('/random_met_object')
@cache(expire=60*5)
async def random_met_object(art_type:str|None = Query(None, max_length=200, regex="^[a-z]+$")) -> dict:
    object_ids = await met_object_search(art_type=art_type or 'landscape')
    match object_ids:
        case list(object_ids):
            for _ in range(10):
                (pick,) = sample(object_ids,1)
            
                try:
                    object = await met_object(pick)
                except InvalidObject:
                    continue
                
                match object:
                    case {"has_image": True}:
                        break
                    case _:
                        continue
            else:
                object = {}
        case _:
            # todo: make this better
            object = {}
    return object

## Nasa Images ## 

@internal_api.get('/nasa_image_search')
@cache(expire=60*60*24)    
async def nasa_image_search(search_term: str = Query(None, max_length=200, regex="^[a-z]+$")) -> list[dict]:
    uri = f"https://images-api.nasa.gov/search?q={quote_plus(search_term)}"
    results = await fetch(uri)
    
    match results:
        case {"collection": {"items": list(items)}}:
            return items
        case _:
            return []
        
@internal_api.get('/random_nasa_image')
@cache(expire=60*5) # 5 minutes
async def random_nasa_image(search_term:str|None = Query(None, max_length=200, regex="^[a-z]+$")) -> dict:
    objects = await nasa_image_search(search_term=search_term or 'nebula')
    match objects:
        case list(objects):
            for _ in range(10):
                (pick,) = sample(objects,1)
                object = parse_nasa_image(pick)
                if object.has_image:
                    object = asdict(object)
                    break
            else:
                print('10 tries exceeded!')
                object = {}
        case _:
            # todo: make this better
            object = {}
    return object

## Asteroids ##

@internal_api.get('/incoming_asteroids')
@cache(expire=60*60*12) # 12 hours
async def incoming_asteroids(n_days_from_now: int = 6) -> list[dict]:
    n_days = min(n_days_from_now, 6)  # todo: make this better
    start_date = datetime.now()
    end_date = start_date + timedelta(days=n_days)
    
    req = await fetch(
                "https://api.nasa.gov/neo/rest/v1/feed",
                params={'api_key': os.environ['NASA_API_KEY'],
                        'start_date': start_date.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d')}
            )
    if type(req) is not dict:
        raise BadRequestResponseError(f"Expected dict, got {req}")
        
    return [asdict(rock) for rock in parse_asteroid_request(req)]
    
@internal_api.get('/asteroid_plot_data')
@cache(expire=60*60*12) # 12 hours
async def asteroid_plot_data(n_days_from_now: int = 6) -> dict:
    asteroids = await incoming_asteroids(n_days_from_now=n_days_from_now)
    
    datasets = {'potentially_hazardous': [],
                'safe': []}
    
    if type(asteroids) is list:
        for rock in asteroids:
            parsed = dict(
                x=rock['approach_date'].timestamp(),
                y=rock['miss_distance_km'],
                width=rock['width_m'],
                name=rock['name'],
                speed=rock['velocity_km_s'])
            if rock['potentially_hazardous']:
                datasets['potentially_hazardous'].append(parsed)
            else:
                datasets['safe'].append(parsed)

    # This is needed for charts.js
    return dict(datasets=[
        dict(label=k, data=d) for k,d in datasets.items()])
    
## Sunset Gifs ##

# This is cached internally, because the response cache-ing was suss
@internal_api.get('/recent_sunset_gif', response_class=RedirectResponse)
async def recent_sunset_gif() -> RedirectResponse:
    gif_url = get_recent_sunset_gif()
    return RedirectResponse(gif_url)
    
## Post Search ##

@internal_api.get('/search')
async def search(query: str | None = Query(None, max_length=200)) -> list[dict]:
    if not query:
        return []
    search_results = await asyncify(repeated_query_attempts()(search_posts))(match_str=query, engine=engine)
    return search_results

## CMEs ##

@internal_api.get('/coronal_mass_ejections')
async def coronal_mass_ejections(background_tasks: BackgroundTasks, start_date: datetime|None = None, end_date: datetime|None = None) -> list[CoronalMassEjection]:
    background_tasks.add_task(update_cmes_with_lookback, api_key=os.environ['NASA_API_KEY'], engine=engine)
    return await asyncify(get_cme_data)(start_date=start_date, end_date=end_date, engine=engine)
    
@internal_api.get('/cme_table')
async def cme_table(background_tasks: BackgroundTasks, n_days: int|None = None, start_date: datetime|None = None, end_date: datetime|None = None) -> HTMLResponse:
    background_tasks.add_task(update_cmes_with_lookback, api_key=os.environ['NASA_API_KEY'], engine=engine)

    table = await asyncify(build_cme_table)(engine=engine, n_days=n_days, start_date=start_date, end_date=end_date)
    
    return HTMLResponse(table.as_raw_html(), status_code=200)
    
## Exoplanets ##

@internal_api.get('/all_exoplanet_system_names')
@cache(expire=60*60*12)  # 12 hours
async def all_exoplanet_system_names(background_tasks: BackgroundTasks) -> list[str]:
    background_tasks.add_task(update_exoplanets_with_min_date, engine=engine)
    return await asyncify(get_all_system_names)(engine=engine)
    
@internal_api.get('/exoplanetary_system')
@cache(expire=60*60*12, coder=PickleCoder)  # 12 hours
async def exoplanetary_system(background_tasks: BackgroundTasks, host_star: str) -> list[ExoPlanet]:
    background_tasks.add_task(update_exoplanets_with_min_date, engine=engine)
    return await asyncify(get_system)(engine=engine, host_star=host_star)