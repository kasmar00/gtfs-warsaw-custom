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
                "C01",
                "C01A",
                "C01B",
                "C01C",
                "C01D",
                "C01F",
                "C02",
                "P01",
                "P01R",
                "P02",
                "P02R",
                "P03",
                "P03R",
                "P04",
                "P04A",
                "P04R",
                "P04RA",
                "P05",
                "P05R",
                "P06",
                "P06R",
                "P07",
                "P07R",
                "P08",
                "P08A",
                "P08S",
                "P08R",
                "P08RA",
                "P08RS",
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
