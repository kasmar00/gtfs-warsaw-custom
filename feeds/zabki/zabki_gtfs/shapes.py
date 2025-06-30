import impuls
from impuls.model import Date, TimePoint, ShapePoint
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, Date, Route, Stop, StopTime, TimePoint, Trip
import json


class LoadShapes(impuls.Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            shapes = [
                "Z2M"
            ]
            for shape in shapes:
                self.create_shapes(shape, r.db)
    

    def create_shapes(self, route, db: DBConnection) -> None:
        with open(f"data/{route}.json") as f:
            d = json.load(f)

            db.raw_execute(
                "INSERT INTO shapes (shape_id) VALUES (?)",
                (route,)
            )

            for i, point in enumerate(d):
                db.create(
                    ShapePoint(
                        shape_id=route,
                        sequence=i,
                        lat=point[1],
                        lon=point[0],
                    )
                )
                

# r=api.query('relation/14327881')
# [x.geometry() for x in r.members()]
# geo = [x.geometry().get("coordinates") for x in r.members() if x.tag("highway")!="platform" and x.type() != 'node']
# flatten = [x for sub in geo for x in sub]