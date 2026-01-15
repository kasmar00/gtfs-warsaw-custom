from typing import NamedTuple, Optional
import impuls
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, Date, Route, Stop, StopTime, TimePoint, Trip
import pandas as pd
import uuid
from .consts import WEEKDAY_CAL_ID, SAT_CAL_ID, SUN_CAL_ID, START_DATE, END_DATE


class HeadSignDefinition(NamedTuple):
    initial: str
    change_stop_id: Optional[str]
    final: Optional[str]
    legacy_headsign: Optional[str]

    def get_headsign(self, is_after_change_stop):
        if is_after_change_stop:
            return self.final
        else:
            return self.initial

HEADSIGN_M1 = HeadSignDefinition("Serbinów", "182", "Plac Dworcowy", "Plac Dworcowy przez Serbinów, Nowe Miasto")
HEADSIGN_M2 = HeadSignDefinition("Serbinów", "182", "Plac Dworcowy", "Plac Dworcowy przez Serbinów, Szpital")
HEADSIGN_M3 = HeadSignDefinition("Rondo Żołnierzy Wyklętych", None, None, "Rondo Żołnierzy Wyklętych")
HEADSIGN_M3R = HeadSignDefinition("Osiedlowa", None, None, "Osiedlowa")
HEADSIGN_M4 = HeadSignDefinition("Spacerowa", "188", "Plac Dworcowy", "Plac Dworcowy przez Spacerowa")


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
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            )

            r.db.create(
                Calendar(
                    id=SAT_CAL_ID,
                    saturday=True,
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            )

            r.db.create(
                Calendar(
                    id=SUN_CAL_ID,
                    sunday=True,
                    start_date=START_DATE,
                    end_date=END_DATE,
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
                ("M1-weekday.txt", WEEKDAY_CAL_ID, "1", HEADSIGN_M1, "M1", "M1A", ""),
                ("M1-saturday.txt", SAT_CAL_ID, "1", HEADSIGN_M1, "M1", "", ""),
                ("M1-sunday.txt", SUN_CAL_ID, "1", HEADSIGN_M1, "M1", "", ""),

                ("M2-weekday.txt", WEEKDAY_CAL_ID, "2", HEADSIGN_M2, "M2", "", ""),
                ("M2-saturday.txt", SAT_CAL_ID, "2", HEADSIGN_M2, "M2", "", ""),
                ("M2-sunday.txt", SUN_CAL_ID, "2", HEADSIGN_M2, "M2", "", ""),

                ("M3-weekday.txt", WEEKDAY_CAL_ID, "3", HEADSIGN_M3, "M3", "", "0"),
                ("M3-saturday.txt", SAT_CAL_ID, "3", HEADSIGN_M3, "M3", "", "0"),
                ("M3-sunday.txt", SUN_CAL_ID, "3", HEADSIGN_M3, "M3", "", "0"),
                ("M3R-weekday.txt", WEEKDAY_CAL_ID, "3", HEADSIGN_M3R, "M3R", "", "1"),
                ("M3R-saturday.txt", SAT_CAL_ID, "3", HEADSIGN_M3R, "M3R", "", "1"),
                ("M3R-sunday.txt", SUN_CAL_ID, "3", HEADSIGN_M3R, "M3R", "", "1"),

                ("M4-weekday.txt", WEEKDAY_CAL_ID, "4", HEADSIGN_M4, "M4", "M4A", ""),
                ("M4-saturday.txt", SAT_CAL_ID, "4", HEADSIGN_M4, "M4", "M4R", ""),
                ("M4-sunday.txt", SUN_CAL_ID, "4", HEADSIGN_M4, "M4", "", ""),
            ]

            for file in files:
                self.create_trips_from_file(file[0], file[1], file[2], file[3], file[4], file[5], file[6], r.db)

    def create_trips_from_file(self, file, calendar, route, headsign: HeadSignDefinition, shape, shape_alt, direction, db: DBConnection):
        after_midnight = False
        has_blocks = False
        data = (
            pd.read_csv(f"data/{file}", sep="\t", header=None)
            .transpose()
            .values.tolist()
        )
        stops = data[0]
        for stop in stops:
            if (stop == "block"):
                has_blocks = True
                continue
            self.create_stop(int(stop), db)
        for trip in data[1:]:
            actual_shape = (shape_alt if trip[1] == "~" and shape_alt else shape) if shape else None

            first_non_empty_time = next(time for time in trip[1:] if time != "~")
            trip_id = f"{calendar}_{actual_shape}_{first_non_empty_time}"

            db.create(
                Trip(
                    id=trip_id,
                    route_id=route,
                    calendar_id=calendar,
                    short_name=route,
                    headsign=headsign.legacy_headsign,
                    block_id=trip[0] if has_blocks else None,
                    shape_id=actual_shape,
                    direction=Trip.Direction.INBOUND if direction == "1" else Trip.Direction.OUTBOUND if direction == "0" else None
                )
            )

            is_after_headsign_change = False

            for i in range(len(trip)):
                if (i == 0 and has_blocks):
                    continue
                if trip[i] == "~":
                    continue
                if i + 1 < len(trip) and stops[i] == stops[i + 1]:
                    continue

                departure_time: TimePoint = _hour_to_time_point(trip[i])

                # Hack for trips finishing after midnight
                if (
                    i - 1 >= (1 if has_blocks else 0)
                    and trip[i - 1] != "~"
                    and departure_time < _hour_to_time_point(trip[i - 1])
                ) or (after_midnight):
                    after_midnight = True
                    departure_time += TimePoint(hours=24)

                if i - 1 >= 0 and stops[i] == stops[i - 1]:
                    arrival_time = _hour_to_time_point(trip[i - 1])
                else:
                    arrival_time = departure_time

                if stops[i] == headsign.change_stop_id:
                    is_after_headsign_change = True

                db.create(
                    StopTime(
                        trip_id=trip_id,
                        stop_id=stops[i],
                        stop_sequence=i,
                        arrival_time=arrival_time,
                        departure_time=departure_time,
                        stop_headsign=headsign.get_headsign(is_after_headsign_change),
                    )
                )

    def create_stop(self, stop_code, db: DBConnection) -> None:
        if stop_code not in self.saved_stops:
            self.saved_stops.add(stop_code)
            db.create(Stop(stop_code, "a", 0.0, 0.0, stop_code))


def _hour_to_time_point(time: str) -> TimePoint:
    hour, minute = time.split(":")
    return TimePoint(hours=int(hour), minutes=int(minute))
