from datetime import datetime, timedelta
from types import CoroutineType
from sqlmodel import Field, SQLModel, text, Session, select, desc, between
from sqlalchemy.engine.base import Engine
from asyncer import asyncify
import polars as pl
import great_tables
from utils import fetch, BadRequestResponseError

##################
# HORRIFIC PATCH #
##################
def _make_one_col_table(vals) -> great_tables.GT:
    """
    Create a one-column table from a list of values.

    Parameters
    ----------
    x
        The list of values to be converted into a table.

    Returns
    -------
        GT: The GT object representing the one-column table.
    """

    # Upgrade a single value to a list
    if not isinstance(vals, (tuple, list, pl.Series)):
        vals = [vals]
    elif isinstance(vals, tuple):
        # anticipating a tuple may be too defensive
        vals = list(vals)

    # TODO: remove pandas. if vals is not a SeriesLike, then we currently
    # convert them to a pandas Series for backwards compatibility.
    df = pl.DataFrame(pl.Series(values=vals, name="x"))

    # Convert the list to a Pandas DataFrame and then to a GTData object
    gt_obj = great_tables.GT(df, auto_align=False)
    return gt_obj

great_tables._formats_vals._make_one_col_table = _make_one_col_table # type: ignore

######################
# END HORRIFIC PATCH #
######################

class CoronalMassEjection(SQLModel, table=True):
    activity_id: str = Field(primary_key=True)
    link: str
    timestamp: datetime
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
    )
    speed_km_s: float
    cme_type: str
    n_analyses: int
    analysis_submission_time: datetime
    
def parse_raw_cme_data(raw_cme_data: dict) -> CoronalMassEjection|None:
    match raw_cme_data:
        # This is a bit creative
        # we take the last "most accurate"
        # because, anecdotally,
        # those tend to be later
        case {
            'activityID': activity_id,
            'link': link,
            'cmeAnalyses': [*_,
                {
                    "isMostAccurate":True,
                    "time21_5": timestamp,
                    "speed": speed_km_s,
                    "type": cme_type,
                    "submissionTime": analysis_submission_time
                }] as all_analyses
            }:
                return CoronalMassEjection(
                    activity_id=activity_id,
                    link=link,
                    timestamp=datetime.strptime(timestamp,'%Y-%m-%dT%H:%MZ'),
                    cme_type=cme_type,
                    speed_km_s=speed_km_s,
                    n_analyses=len(all_analyses),
                    analysis_submission_time=datetime.strptime(analysis_submission_time,'%Y-%m-%dT%H:%MZ'),
                )
        case _:
            return None
            
async def query_cme_activity(start_date: datetime, end_date: datetime, api_key: str) -> list[dict]:
    resp = await fetch(
        f'https://api.nasa.gov/DONKI/CME?startDate={start_date.strftime("%Y-%m-%d")}&endDate={end_date.strftime("%Y-%m-%d")}&api_key={api_key}'
    )
    if type(resp) is not list:
        raise BadRequestResponseError(f"Expected list[dict], got {resp}")
    return resp
    
def merge_cmes(cme_data: list[CoronalMassEjection], engine: Engine) -> None:
    with Session(engine) as session:
        for cme in cme_data:
            session.merge(cme)
        session.commit()
    
async def upload_cme_activity(start_date: datetime, end_date: datetime, api_key: str, engine: Engine) -> None:
    raw_cme_data = await query_cme_activity(start_date=start_date, end_date=end_date, api_key=api_key)
    parsed_cme_data = [cme_row for cme_data in raw_cme_data if (cme_row := parse_raw_cme_data(cme_data))]
    if parsed_cme_data:
        await asyncify(merge_cmes)(cme_data=parsed_cme_data, engine=engine)

def get_most_recent_update(engine: Engine) -> datetime|None:
    with Session(engine) as session:
        most_recent = session.exec(select(CoronalMassEjection).order_by(desc(CoronalMassEjection.updated_at))).first()
    if most_recent:
        return most_recent.updated_at
        
async def update_cmes_with_lookback(api_key: str, engine: Engine, lookback_days=10) -> None:
    now = datetime.now()
    most_recent_update = get_most_recent_update(engine)
    if most_recent_update and now.date() <= most_recent_update.date():
        return None
    await upload_cme_activity(
        start_date=now - timedelta(days=lookback_days),
        end_date=now,
        api_key=api_key,
        engine=engine
    )
    
def get_cme_data(engine: Engine, start_date: datetime|None = None, end_date: datetime|None = None) -> list[CoronalMassEjection]:
    start_date = start_date or datetime.strptime('1990-01-01', '%Y-%m-%d')
    end_date = end_date or datetime.strptime('3000-01-01', '%Y-%m-%d')
    
    with Session(engine) as session:
        cmes = session.exec(select(CoronalMassEjection).filter(between(CoronalMassEjection.timestamp, start_date, end_date))).all()
        
    return list(cmes) or []
    
def build_cme_table(engine: Engine, n_days: int|None = None, start_date: datetime|None = None, end_date: datetime|None = None) -> great_tables.GT:
    
    if n_days and n_days > 0:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=n_days)
    else:
        start_date = start_date or datetime.strptime('1990-01-01', '%Y-%m-%d')
        end_date = end_date or datetime.strptime('3000-01-01', '%Y-%m-%d')
    
    query = """
    with 
    filtered_cmes as (
        select
            *
        from coronalmassejection
        where timestamp between :start_date and :end_date
    ),
    cme_types as (
        select 'S' as cme_type, 1 as rank
        union
        select 'C' as cme_type, 2 as rank
        union
        select 'O' as cme_type, 3 as rank
        union
        select 'R' as cme_type, 4 as rank
        union
        select 'ER' as cme_type, 5 as rank
    ),
    bounds as (
        select 
            min(timestamp) as min_time,
            max(timestamp) as max_time
        from filtered_cmes
    )
    select
        ct.rank, 
        ct.cme_type, 
        count(activity_id) as cme_count,
        min(speed_km_s) as slowest, 
        avg(speed_km_s) as average,
        max(speed_km_s) as fastest,
        group_concat((unixepoch(timestamp) - unixepoch(min_time)) order by timestamp) as xs,
        group_concat(speed_km_s order by timestamp) as speeds,
        unixepoch(max_time) - unixepoch(min_time) as max_time_bound,
        min_time,
        max_time
        from cme_types ct
        left join filtered_cmes cme
            on ct.cme_type = cme.cme_type
        cross join bounds
        group by rank, ct.cme_type
        order by rank
    """
    
    df = pl.read_database(
        query=query, 
        connection=engine.connect(), 
        execute_options={"parameters": {"start_date": start_date, "end_date": end_date}},
    )
    
    start_date_actual = df.select(pl.first("min_time").str.to_datetime()).item()
    end_date_actual = df.select(pl.first("max_time").str.to_datetime()).item()
    
    table = great_tables.GT(
        df.drop(['rank', 'max_time_bound', 'xs', 'min_time', 'max_time']), rowname_col="cme_type"
    ).tab_header(
        title="Coronal Mass Ejections",
        subtitle=f"CMEs from {start_date_actual.strftime('%B %d, %Y')} to {end_date_actual.strftime('%B %d, %Y')} ({(end_date_actual - start_date_actual).days + 1:.0f} days)"
    ).tab_options(
        table_background_color="#fffff8"
    ).tab_stubhead(
        label="Type"
    ).fmt_integer(columns=[
        'cme_count',
        'slowest',
        'average',
        'fastest'
    ]).fmt_nanoplot(
        columns="speeds",
        # expand_y=[df.select('slowest').max().item(), df.select('fastest').max().item()],
        options=great_tables.nanoplot_options(
            show_data_area=False,
            data_point_radius=1,
            data_point_stroke_width=1,
        )
        
    ).cols_label(
        cme_count=great_tables.html("CME<br>Count"),
        slowest=great_tables.html("Slowest Speed,<br>km/s"),
        average=great_tables.html("Average Speed,<br>km/s"),
        fastest=great_tables.html("Fastest Speed,<br>km/s"),
        speeds=great_tables.html("Speed for each event")  
    ).sub_missing(
        columns=[ "slowest", "average", "fastest", "speeds"],
        missing_text=""
    ).tab_source_note(
        source_note=great_tables.md(
            "Courtesty of the fine folks at the The [Space Weather Database Of Notifications, Knowledge, Information](https://ccmc.gsfc.nasa.gov/tools/DONKI/) (DONKI). Their acronym, not mine. ")
    ).tab_source_note(
        source_note=great_tables.md(
            "CMEs are classified as S (slow), C (common), O (occasional), R (rare), ER (extemely rare) [depending on speed](https://agupubs.onlinelibrary.wiley.com/doi/full/10.1002/swe.20058).")
    )
    
    return table