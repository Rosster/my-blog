import re
import os
from functools import cache

import cloudinary
from cloudinary.search import Search
from utils import BadRequestResponseError

cloudinary.config(cloud_name = os.getenv('CLOUD_NAME'),
                  api_key=os.getenv('API_KEY'),
                  api_secret=os.getenv('API_SECRET'))

@cache
def get_recent_sunset_gif(folder='sunset_gifs') -> str:
    folder_search_results = (Search()
        .expression(f'resource_type:image AND folder={folder}')
        .sort_by('public_id', 'desc')
        .max_results('30')
        .execute()
    )
    
    match folder_search_results:
        case {
            'resources': [
                {
                    'secure_url': image_url
                }, *_
            ]
        }:
            url = re.sub(r"(?<=upload/).*?(?=/sunset_gifs)", 'f_auto,fl_lossy/q_60', image_url) 
            return url
    raise BadRequestResponseError(folder_search_results)