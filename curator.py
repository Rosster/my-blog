from datetime import datetime
from dataclasses import dataclass
from urllib.parse import quote
from utils import BadRequestResponseError

class InvalidObject(Exception):
    pass

@dataclass
class ArtObject:
    id: str
    attribution: str | None
    attribution_quoted: str | None
    secondary_attribution: str
    secondary_attribution_quoted: str
    title: str
    title_quoted: str
    description: str | None
    raw_creation_date: str
    structured_creation_date: datetime | None
    thumbnail_image_url: str | None
    small_image_url: str | None
    large_image_url: str | None
    largest_image_url: str | None
    smallest_image_url: str | None
    has_image: bool
    meta: dict | None
    

def parse_met_art_object(raw_object: dict) -> ArtObject:
    
    match raw_object:
        case {
            "objectID": object_id,
            "artistDisplayName": attribution,
            "culture": secondary_attribution,
            "title": title,
            "objectDate": raw_creation_date,
            "primaryImageSmall": small_image_url,
            "primaryImage": large_image_url
        }:
            return ArtObject(
                id=str(object_id),
                attribution=str(attribution),
                attribution_quoted=quote(attribution),
                secondary_attribution=str(secondary_attribution),
                secondary_attribution_quoted=quote(secondary_attribution),
                title=title,
                title_quoted=quote(title),
                description=None,
                raw_creation_date=raw_creation_date,
                structured_creation_date=None,
                thumbnail_image_url=None,
                small_image_url=small_image_url,
                large_image_url=large_image_url,
                largest_image_url=large_image_url or small_image_url,
                smallest_image_url=small_image_url or large_image_url,
                has_image=any([small_image_url, large_image_url]),
                meta=raw_object
            )
        case {'message': 'Not a valid object'}:
            raise InvalidObject()
        case _:
            print(raw_object)
            raise BadRequestResponseError(raw_object)
    
            
def parse_nasa_image(raw_object: dict) -> ArtObject:
    
    match raw_object:
        case {
            "data": [{
                "date_created": raw_creation_date,
                "description": description,
                "title": title,
                "nasa_id": object_id,
            } as data],
            "links": list(links)
        }:
            # filter list--although all of these should be images
            links = [image_dict for image_dict in links if image_dict.get('render') == 'image']
            links.sort(key=lambda image_dict: float(image_dict.get("size", float("inf"))))
            
            match links:
                case [{'href': href}]:
                    large_image_url = href
                    small_image_url = None
                    thumbnail_image_url = None
                case [{'href': small_href}, {'href': large_href}]:
                    small_image_url = small_href
                    large_image_url = large_href
                    thumbnail_image_url = None
                case [{'href': thumb_href}, {'href': small_href}, *rest]:
                    # This is messy, but I dunno how to match to
                    # none or many
                    large_href = rest[0]['href']
                    small_image_url = small_href
                    large_image_url = large_href
                    thumbnail_image_url = thumb_href
                case _:
                    small_image_url = None
                    large_image_url = None
                    thumbnail_image_url = None
            
            attribution = data.get('photographer', data.get('secondary_creator', data.get('center', 'Unknown')))
            secondary_attribution = data.get('center', 'Unknown')
            
            return ArtObject(
                id=str(object_id),
                attribution=str(attribution),
                attribution_quoted=quote(attribution),
                secondary_attribution=str(secondary_attribution),
                secondary_attribution_quoted=quote(secondary_attribution),
                title=title,
                title_quoted=quote(title),
                description=description,
                raw_creation_date=raw_creation_date,
                structured_creation_date=None,
                thumbnail_image_url=thumbnail_image_url,
                small_image_url=small_image_url,
                large_image_url=large_image_url,
                largest_image_url=large_image_url or small_image_url,
                smallest_image_url=small_image_url or large_image_url,
                has_image=any([small_image_url, large_image_url, thumbnail_image_url]),
                meta=raw_object
            )
        case _:
            raise Exception("Problem", str(raw_object))