from dataclasses import dataclass
from datetime import datetime
from utils import BadRequestResponseError


@dataclass
class Asteroid:
    link: str
    name: str
    width_m: float
    velocity_km_s: float
    approach_date: datetime
    potentially_hazardous: bool
    miss_distance_km: float
    
    
def parse_asteroid_request(near_earth_object_response: dict) -> list[Asteroid]:
    # This dict should have 3 top level keys
    # links - dict provides linkes to page through chunks of days
    # element_count - int the count of asteroids
    # near_earth_objects - dict the data on the objects (the keys are YYYY-MM-DD days)
    #   each day has a list of objects
    
    asteroids = []
    if near_earth_objects := near_earth_object_response.get('near_earth_objects'):
        for day, rocks in near_earth_objects.items():
            for rock in rocks:
                match rock:
                    case {
                        'nasa_jpl_url': link,
                        'name': name,
                        'is_potentially_hazardous_asteroid': potentially_hazardous,
                        'estimated_diameter': {
                            'meters': {
                                'estimated_diameter_min': estimated_diameter_min,
                                'estimated_diameter_max': estimated_diameter_max
                            }
                        },
                        'close_approach_data': [{
                            'epoch_date_close_approach': epoch_date_close_approach,
                            'relative_velocity': {
                                'kilometers_per_second': velocity_km_s
                            },
                            'miss_distance': {
                                'kilometers': miss_distance_km
                            }
                        }, *_]
                    }:
                        asteroids.append(
                            Asteroid(
                                link=link,
                                name=name.strip('()'),
                                width_m=(estimated_diameter_min+estimated_diameter_max)/2,
                                velocity_km_s=float(velocity_km_s),
                                approach_date=datetime.fromtimestamp(epoch_date_close_approach/1000.0),
                                potentially_hazardous=bool(potentially_hazardous),
                                miss_distance_km=float(miss_distance_km)
                            )
                        )
                        
    else:
        raise BadRequestResponseError(near_earth_object_response)
    
    return sorted(asteroids, key=lambda a: a.approach_date)