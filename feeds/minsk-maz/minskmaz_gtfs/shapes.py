import impuls
from impuls.model import ShapePoint
from impuls import DBConnection, TaskRuntime
import json


class LoadShapes(impuls.Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            shapes = [
                "M1",
                "M1R",
                "M2",
                "M2R",
                "M3-weekday",
                "M3R-weekday",
                "M3",
                "M3R",
                "M4",
                "M4R",
                "Z3",
                "Z3R",
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
