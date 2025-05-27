import impuls
from impuls.model import Date, TimePoint
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, Date, Route, Stop, StopTime, TimePoint, Trip
import pandas as pd
import uuid

WEEKDAY_CAL_ID = "1"
SAT_CAL_ID = "2"
SUN_CAL_ID = "3"


class LoadTrips(impuls.Task):
    def __init__(self) -> None:
        super().__init__()
        self.saved_stops = set[str]()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            # calendar
            r.db.create(
                Calendar(
                    id=WEEKDAY_CAL_ID,
                    monday=True,
                    tuesday=True,
                    wednesday=True,
                    thursday=True,
                    friday=True,
                    start_date="2024-10-12",
                    end_date="2025-12-31",
                )
            )

            r.db.create(
                Calendar(
                    id=SAT_CAL_ID,
                    saturday=True,
                    start_date="2024-10-12",
                    end_date="2025-12-31",
                )
            )

            r.db.create(
                Calendar(
                    id=SUN_CAL_ID,
                    sunday=True,
                    start_date="2024-10-12",
                    end_date="2025-12-31",
                )
            )

            # routes
            for route in ["1", "2", "3", "4"]:
                r.db.create(
                    Route(
                        id=route,
                        agency_id="0",
                        short_name="1",
                        long_name="1",
                        type=Route.Type.BUS,
                    )
                )
            

            files = [
                ("Z1-weekday.txt", WEEKDAY_CAL_ID, "1"),
                ("Z1-saturday.txt", SAT_CAL_ID, "1"),
                ("Z1-sunday.txt", SUN_CAL_ID, "1"),
                ("Z2M-weekday.txt", WEEKDAY_CAL_ID, "2"),
                ("Z3-weekday.txt", WEEKDAY_CAL_ID, "3"),
                ("Z3-saturday.txt", SAT_CAL_ID, "3"),
                ("Z3-sunday.txt", SUN_CAL_ID, "3"),
                ("Z4M-weekday.txt", WEEKDAY_CAL_ID, "4"),
                ("Z4M-saturday.txt", SAT_CAL_ID, "4"),
                ("Z4M-sunday.txt", SUN_CAL_ID, "4"),
            ]

            for file in files:
                self.create_trips_from_file(file[0], file[1], file[2], r.db)

    def create_trips_from_file(self, file, calendar, route, db: DBConnection):
        data = (
            pd.read_csv(f"data/{file}", sep="\t", header=None)
            .transpose()
            .values.tolist()
        )
        stops = data[0]
        for stop in stops:
            self.create_stop(stop, db)
        for trip in data[1:]:

            trip_id = str(uuid.uuid4())

            db.create(
                Trip(
                    id=trip_id,
                    route_id=route,
                    calendar_id=calendar,
                    headsign="???",
                    short_name=route,
                )
            )

            for i in range(len(trip)):
                if trip[i] == "~":
                    continue
                if i+1 < len(trip) and stops[i]==stops[i+1]:
                    continue

                hour, minute = trip[i].split(":")

                departure_time = TimePoint(hours=int(hour), minutes=int(minute))
                if i-1>=0 and stops[i]==stops[i-1]:
                    prev_h, prev_m = trip[i-1].split(":")
                    arrival_time = TimePoint(hours=int(prev_h), minutes=int(prev_m))
                else:
                    arrival_time = departure_time


                db.create(
                    StopTime(
                        trip_id=trip_id,
                        stop_id=stops[i],
                        stop_sequence=i,
                        arrival_time=arrival_time,
                        departure_time=departure_time,
                    )
                )

    def create_stop(self, stop_code, db: DBConnection) -> None:
        if stop_code not in self.saved_stops:
            self.saved_stops.add(stop_code)
            db.create(Stop(stop_code, "a", 0.0, 0.0, stop_code))
