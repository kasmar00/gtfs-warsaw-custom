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
                "C1A",
                "C1B",
                "C1C",
                "C1D",
                "C1E",
                "C1F",
                "C2",
                "P1",
                "P1R",
                "P2",
                "P2R",
                "S1",
                "S1R",
                "SP3",
                "SP3R",
                "ST1",
                "ST1R",
                "ST2",
                "ST2R",
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
