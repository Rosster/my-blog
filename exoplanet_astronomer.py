import random
from datetime import datetime, timedelta
import math
from itertools import product
import os
import pytz
from sqlmodel import Field, SQLModel, text, Session, select, desc, func
from sqlalchemy.engine.base import Engine
from asyncer import asyncify
from utils import fetch

####################
# Model Definition #
####################

class ExoPlanet(SQLModel, table=True):
    # This is a hash of hostname and planet name
    # literally `hash(hostname+pl_name)`
    host_name: str = Field(primary_key=True)
    planet_name: str = Field(primary_key=True)
    radius_in_earths: float
    mass_in_earths: float
    density_g_cm3: float
    planet_emoji: str
    publication_update_date: datetime
    discovery_publication_date: datetime
    is_circumbinary: bool
    n_stars_in_system: int
    n_planets_in_system: int
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
    )
    posted_to_bsky_at: datetime|None = Field(default=None)

###################
# Parsing Helpers #
###################

def parse_datetime(pubdate_str: str) -> datetime:
    # We have to do this because the datetimes 
    # aren't consistent and for some silly reason 
    # it breaks pandas to datetime
    
    match str(pubdate_str).split('-'):
        case [year, month]:
            return datetime(year=int(year), month=int(month) or 1, day=1, tzinfo=pytz.utc)
        case [year, month, day]:
            return datetime(year=int(year), month=int(month) or 1, day=int(day), tzinfo=pytz.utc)
        case _:
            return datetime(year=2000, month=1, day=1, tzinfo=pytz.utc)

def get_planet_emoji(pl_masse: float) -> str:
    # Based on https://www.planetary.org/articles/0415-favorite-astro-plots-4-classifying-exoplanets
    # But with some creative license because I'm giving small planets a moon emoji

    if pl_masse < 0.5:
        return "🌖"
    elif pl_masse < 0.9:
        return "🌗"
    elif pl_masse < 1.1:
        return random.sample(['🌎', '🌍', '🌏'], 1)[0]
    elif pl_masse < 1.5:
        return "🌒"
    elif pl_masse < 2.0:
        return "🌑"
    else:
        return '🪐'

#################
# Post Building #
#################

def build_template(max_orbit: int, is_circumbinary=False) -> str:
    template_rows = []

    # Top half
    for idx in reversed(range(max_orbit)):
        row = ('\u3000' * (max_orbit - idx - 1)) + f'{{loc_{idx}_7}}' + ('\u3000' * idx) + f'{{loc_{idx}_0}}' + ('\u3000' * idx) + f'{{loc_{idx}_1}}' + ('\u3000' * (max_orbit - idx - 1))
        template_rows.append(row)

    if is_circumbinary:
        if max_orbit % 2 == 0:
            top_str = ''.join(f'{{loc_{idx}_6}}\u3000' for idx in reversed(range(0,max_orbit,2))) + '☀️' + ''.join(f'{{loc_{idx}_2}}\u3000' for idx in range(0,max_orbit,2))
            bottom_str = ''.join(f'\u3000{{loc_{idx}_6}}' for idx in reversed(range(1,max_orbit,2))) + '☀️' + ''.join(f'\u3000{{loc_{idx}_2}}' for idx in range(1,max_orbit,2))
        else:
            top_str = ''.join(f'{{loc_{idx}_6}}\u3000' for idx in reversed(range(0,max_orbit,2))).strip() + '☀️' + ''.join(f'\u3000{{loc_{idx}_2}}' for idx in range(1,max_orbit,2))
            bottom_str = ''.join(f'\u3000{{loc_{idx}_6}}' for idx in reversed(range(1,max_orbit,2))) + '\u3000' + '☀️' + ''.join(f'{{loc_{idx}_2}}\u3000' for idx in range(0,max_orbit,2))
        
        template_rows.extend([top_str, bottom_str])
    else:
        mid_str = ''.join(f'{{loc_{idx}_6}}' for idx in reversed(range(max_orbit))) + '☀️' + ''.join(f'{{loc_{idx}_2}}' for idx in range(max_orbit))
        template_rows.append(mid_str)
    
    # bottom half
    for idx in range(max_orbit):
        row = ('\u3000' * (max_orbit - idx - 1)) + f'{{loc_{idx}_5}}' + ('\u3000' * idx) + f'{{loc_{idx}_4}}' + ('\u3000' * idx) + f'{{loc_{idx}_3}}' + ('\u3000' * (max_orbit - idx - 1))
        template_rows.append(row)
    return '\n'.join(template_rows)

def render_system(system: list[ExoPlanet], min_render_size_orbits=8) -> str:
    system = sorted(system, key=lambda p: p.radius_in_earths)
    n_planets = len(system)
    render_size_orbits = max(min_render_size_orbits, n_planets)
    
    if n_planets > 1:
        tie_broken_radii = [p.radius_in_earths + (0.0001 * i) for i,p in enumerate(system)]
        min_radius = min(tie_broken_radii)
        max_radius = max(tie_broken_radii)
        normalized_radii = [(radius - min_radius) / (max_radius - min_radius) for radius in tie_broken_radii]
    else:
        normalized_radii = [0]
    
    # This code distributes the planets in the system to the orbits
    orbits = ['\u3000'] * render_size_orbits
    for idx, (normed_radius, planet) in enumerate(zip(normalized_radii, system)):
        proportionate_normed_radius = normed_radius * (render_size_orbits - 1)
        
        start_pos = 0 if math.floor(proportionate_normed_radius) == 0 else math.floor(proportionate_normed_radius) - 1
        for arr_idx, arr_val in enumerate(orbits[start_pos:]):
            arr_idx = arr_idx + start_pos
            if arr_val == '\u3000':
                orbits[arr_idx] = planet.planet_emoji
                break
            else:
                continue
                
    # define positions, we have the radii, 
    # we just want to randomize positions
    
    # initialize fields to blank, we only have 8 orientations, hence the range 8
    fields = {f"loc_{a}_{b}":'\u3000' for a,b in product(range(render_size_orbits), range(8))}
    for orbit_radius_position, orbit_emoji in enumerate(orbits):
        orientation = random.randint(0,7)
        fields[f"loc_{orbit_radius_position}_{orientation}"] = orbit_emoji
    
    template = build_template(
        max_orbit=render_size_orbits, 
        is_circumbinary=max(p.is_circumbinary for p in system)
    )
    
    render = template.format(**fields)
    
    # just dropping empty lines
    compressed_lines = []
    lines = render.strip('\n').split('\n')

    for idx, line in enumerate(lines):
        if line.strip():
            compressed_lines.append(line.rstrip())
        elif not compressed_lines:
            continue
        elif all(not l.strip() for l in lines[idx:]):
            break
        else:
            compressed_lines.append(line.rstrip())

    return '\n'.join(compressed_lines)    

############################
# DB Interactions Building #
############################

async def get_system_data(incremental_date_str='1608-01-01') -> list[ExoPlanet]:
    system_query = f"""
        select 
            psc.hostname as host_name, 
            psc.pl_name as planet_name, 
            psc.pl_rade as radius_in_earths, 
            psc.pl_bmasse as mass_in_earths, 
            psc.pl_dens as density_g_cm3, 
            psc.disc_pubdate as discovery_publication_date, 
            psc.cb_flag as is_circumbinary, 
            psc.sy_snum as n_stars_in_system, 
            psc.sy_pnum as n_planets_in_system,
            max(ps.rowupdate) as publication_update_date 
        from pscomppars psc join ps on psc.hostname = ps.hostname 
        where ps.rowupdate >= '{incremental_date_str}'
        group by             
            psc.hostname, 
            psc.cb_flag, 
            psc.sy_snum, 
            psc.sy_pnum,
            psc.pl_name, 
            psc.disc_pubdate, 
            psc.pl_rade, 
            psc.pl_bmasse, 
            psc.pl_dens
    """
    planets_raw = await fetch(
        "https://exoplanetarchive.ipac.caltech.edu/TAP/sync",
        params=dict(query=system_query, format='json')
    )   
    planets = [
        ExoPlanet(
            host_name = planet['host_name'],
            planet_name = planet['planet_name'],
            radius_in_earths = planet['radius_in_earths'] or 0,
            mass_in_earths = planet['mass_in_earths'] or 0,
            density_g_cm3 = planet['density_g_cm3'] or 0,
            planet_emoji = get_planet_emoji(planet['mass_in_earths'] or 0),
            publication_update_date = parse_datetime(planet['publication_update_date']),
            discovery_publication_date = parse_datetime(planet['discovery_publication_date']),
            is_circumbinary = bool(planet['is_circumbinary']),
            n_stars_in_system = planet['n_stars_in_system'],
            n_planets_in_system = planet['n_planets_in_system'],
        ) for planet in planets_raw
    ]
    
    return planets
    
def get_most_recent_update(engine: Engine) -> datetime|None:
    with Session(engine) as session:
        most_recent = session.exec(select(ExoPlanet).order_by(desc(ExoPlanet.updated_at))).first()
    if most_recent:
        return most_recent.updated_at
        
def get_all_system_names(engine: Engine) -> list[str]:
    with Session(engine) as session:
        system_names = session.exec(select(ExoPlanet.host_name).distinct()).all()
    return list(system_names)
    
def merge_exoplanets(planet_data: list[ExoPlanet], engine: Engine) -> None:
    with Session(engine) as session:
        for planet in planet_data:
            session.merge(planet)
        session.commit()
    
def get_system(engine: Engine, host_star: str) -> list[ExoPlanet]:
    with Session(engine) as session:
        planets = session.exec(select(ExoPlanet).where(func.lower(ExoPlanet.host_name) == host_star.lower())).all()
    return list(planets)
    
async def update_exoplanets_with_min_date(engine: Engine, min_date: datetime|None = None) -> None:
    # If min date is none then we update relative to the most recent update date
    # If that's none, then we update everything
    # Never do the work if it's already been done on this day
    
    min_date = min_date or get_most_recent_update(engine=engine) # This can be None, that's ok
    now = datetime.now()
    if min_date and now.date() <= min_date.date():
        return None
    
    if not min_date:
        planets = await get_system_data()
    else:
        planets = await get_system_data(incremental_date_str=min_date.strftime('%Y-%m-%d'))
    
    if planets:
        print(f"{len(planets)} planets...")
        await asyncify(merge_exoplanets)(planet_data=planets, engine=engine)
        
def post_system(system: list[ExoPlanet]) -> None:
    from atproto import Client, client_utils  # We do this because it's a massive module
    
    if system:
        system_render = render_system(system)
        system_exoplanet_archive_link = f"https://exoplanetarchive.ipac.caltech.edu/overview/{system[0].host_name}"
        client = Client()
        client.login(
            os.environ.get('EXOPLANET_ACCOUNT_NAME'),
            os.environ.get('EXOPLANET_ACCOUNT_KEY')
        )
        post = client_utils.TextBuilder().link(
            system[0].host_name, system_exoplanet_archive_link
        ).text('\n'+system_render)
        client.send_post(post)
      
async def post_new_exoplanet_system(engine: Engine) -> None:
    await update_exoplanets_with_min_date(engine=engine)
    
    with Session(engine) as session:
        most_recent_post_dt = session.exec(select(func.max(ExoPlanet.posted_to_bsky_at))).first()
    
    if most_recent_post_dt and most_recent_post_dt >= (datetime.now() - timedelta(days=1)):
        return
        
    with Session(engine) as session:
        new_hosts = session.exec(select(ExoPlanet.host_name).where(ExoPlanet.posted_to_bsky_at == None)).all()
        if new_hosts:
            host = random.sample(new_hosts, 1)[0]
            system = get_system(engine=engine, host_star=host)
            post_system(system)
            
            with Session(engine) as session:
                for planet in system:
                    planet.posted_to_bsky_at = datetime.now(pytz.utc)
                    session.merge(planet)
                session.commit()