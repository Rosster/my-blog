from datetime import datetime
import textwrap
import re
from sqlmodel import Field, SQLModel, text, Session, select, desc
from sqlalchemy.engine.base import Engine
import markdown
import smartypants
from bs4 import BeautifulSoup
import jinja2
from fastapi import Request


def suffix(d):
    """From here: https://stackoverflow.com/questions/5891555/display-the-date-like-may-5th-using-pythons-strftime"""
    return 'th' if 11 <= d <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(d % 10, 'th')


def custom_strftime(datetime_format, t):
    """From here: https://stackoverflow.com/questions/5891555/display-the-date-like-may-5th-using-pythons-strftime"""
    return t.strftime(datetime_format).replace('{S}', str(t.day) + suffix(t.day))


class Post(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    immutable_title: str
    title: str
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
    )
    keywords: str  # this'll be a pipe delimited set of words/phrases
    is_draft: bool
    preview: str
    posted_at: datetime | None
    content_html: str | None
    scripts_html: str | None
    content_md: str | None  
    searchable_text: str
    formatted_date: str | None 

class PostRenderer:
    def __init__(self, post_template_path: str, environment: jinja2.Environment):
        with open(post_template_path, 'rt') as post_template:
            raw_template = post_template.read()
        self.environment = environment
        self.raw_template = raw_template
        
    def render_post(self, post: Post, request: Request) -> str:
        # first we render appropriately
        parsed_template = (self.raw_template
            .replace("<<scripts>>", post.scripts_html or '')
            .replace("<<content>>", post.content_html or '')
        )
        
        post_html = self.environment.from_string(parsed_template).render(post=post, request=request)
        
        post_html = smartypants.smartypants(post_html)
        
        return post_html
        
        

###################
# Post Operations #
###################    
        
def parse_post_text(
    immutable_title: str,
    title: str,
    keywords: list[str],
    publish_immediately: bool = False,
    post_md: str | None = None, 
    post_html: str | None = None, 
    **overrides
) -> Post:
        
    if not post_md and not post_html:
        raise Exception("No post text, this is horrible")
    
    if not post_html and post_md:
        post_html = markdown.markdown(post_md)
    elif not post_html:
        raise Exception("No post text, this is horrible")
    
    parsed_post_html = BeautifulSoup(post_html, features="html.parser")
        
    # So now we have some html
    if  preview := overrides.get('preview'):
        preview = str(preview)
    else:
        preview = parsed_post_html.find('p') or ""
        if preview:
            preview = textwrap.dedent(preview.get_text())
            
    # scripts
    scripts = []
    for script in parsed_post_html("script"):
        scripts.append(str(script))
        script.decompose()
    scripts_html = '\n'.join(scripts)
        
    searchable_text = ' '.join(parsed_post_html.get_text().split())
    # We have to do a bit of extra work to ditch the jinja elements
    searchable_text = re.sub(r'\{\%.*?\%\}', '', searchable_text)
    
    posted_at=overrides.get('posted_at', datetime.now()) if publish_immediately else None
    formatted_date = custom_strftime('%B {S}, %Y', posted_at) if posted_at is not None else None
        
    return Post(
        title=title,
        immutable_title=immutable_title,
        preview=str(preview),
        keywords='|'.join(word.replace('|','') for word in keywords),
        is_draft=not publish_immediately,
        posted_at=posted_at,
        content_html=str(parsed_post_html),
        scripts_html=scripts_html,
        content_md=post_md,
        searchable_text=searchable_text,
        formatted_date=formatted_date
    )

def publish_post(post_immutable_title: str, session: Session, posted_at_override: None|datetime = None) -> None:
    # unpublish all
    for post in session.exec(select(Post).where(Post.immutable_title == post_immutable_title)).all():
        post.is_draft = True
    if most_recent_post := session.exec(select(Post).where(Post.immutable_title == post_immutable_title).order_by(desc(Post.updated_at))).first():
        most_recent_post.is_draft = False
        most_recent_post.posted_at = posted_at_override or datetime.now()
        most_recent_post.formatted_date = custom_strftime('%B {S}, %Y', most_recent_post.posted_at)
    session.commit()

def get_all_posts(engine: Engine) -> list[Post]:
    with Session(engine) as session:
        posts = session.exec(select(Post).where(Post.is_draft==0).order_by(desc(Post.posted_at))).all()
    return list(posts)
    
def get_single_post(engine: Engine, immutable_title: str) -> Post|None:
    with Session(engine) as session:
        post = session.exec(
            select(Post).where((Post.immutable_title == immutable_title) and (Post.is_draft == 0)).order_by(desc(Post.updated_at))
        ).first()
    return post

def search_posts(match_str: str, engine: Engine, fields = ('immutable_title','title', 'keywords', 'searchable_text')) -> list:
    match_query = f'''
        with snippets as (
            SELECT 
            {', '.join(fields)},
            {', '.join([f"snippet(post_search, {idx}, '<b>', '</b>', '...', 8) as {field}_snippet" for idx, field in enumerate(fields)])}
            FROM post_search 
            WHERE post_search MATCH :match_str
            order by rank
        )
        select 
            p.*,
            {', '.join([f'{field}_snippet' for field in fields])},
            {', '.join([f'instr(s.{field}, trim({field}_snippet, ".")) = 0 as {field}_match' for field in fields])}
        from snippets s
        join post p 
            on s.immutable_title = p.immutable_title
            and p.is_draft = 0
        '''

    with Session(engine) as session:
        results = session.execute(text(match_query), dict(match_str=match_str))
        columns = results.keys()
        result_values = list(results)
        
    return [dict(zip(columns, result_tuple)) for result_tuple in result_values]
    
####################
# Table Operations #
####################

def setup_search_table(session: Session) -> None:
    sql = """
    DROP TABLE IF EXISTS post_search;
    
    CREATE VIRTUAL TABLE post_search USING fts5 (immutable_title, title, keywords, searchable_text);
    
    INSERT INTO
      post_search (immutable_title, title, keywords, searchable_text)
    SELECT
      immutable_title, 
      title,
      keywords,
      searchable_text
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              immutable_title
            ORDER BY
              updated_at DESC
          ) AS ROW_NUMBER
        FROM
          post
        WHERE is_draft = 0
      )
    WHERE
      ROW_NUMBER = 1;
    """
    
    session.execute(text(sql))
    session.commit()
