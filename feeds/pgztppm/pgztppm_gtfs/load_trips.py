import impuls
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, Date, Route, Stop, StopTime, TimePoint, Trip
import pandas as pd
import uuid
from .consts import WEEKDAY_CAL_ID, HOLIDAY_CAL_ID, SCHOOL_CAL_ID, START_DATE, END_DATE

headsign_C01 = {
    "C01": "Cegłów Szkoła przez Posiadały, Kiczki",
    "C01A": "Cegłów Szkoła przez Posiadały",
    "C01B": "Cegłów Szkoła przez Mienia, Cegłów Szkoła, Posiadały, Kiczki",
    "C01C": "Cegłów Szkoła przez Mienia, Posiadały, Kiczki",
    "C01D": "Cegłów Szkoła przez Kiczki, Posiadały",
    "C01F": "Cegłów Szkoła przez Mienia",
}

headsign_P04 = {
    "P04": "Wólka Piecząca",
    "P04A": "Wólka Piecząca przez Kolonie Stanisławów"
}

headsign_P08 = {
    "P08": "Garczyn Mały Pętla",
    "P08A": "Garczyn Mały Pętla przez Budy Barcząckie",
    "P08S": "Kałuszyn Centrum",
}

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
                    id=HOLIDAY_CAL_ID,
                    saturday=True,
                    sunday=True,
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            )

            r.db.create(
                Calendar(
                    id=SCHOOL_CAL_ID,
                    monday=True,
                    tuesday=True,
                    wednesday=True,
                    thursday=True,
                    friday=True,
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            )

            # routes
            for route in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]:
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
                ("C01.tsv", None, "1", headsign_C01, None, ""),

                ("C02.tsv", SCHOOL_CAL_ID, "2", "Cegłów Szkoła przez Kiczki", "C02", ""),

                ("P01.tsv", None, "3", "Cisówka Pętla", "P01", "0"),
                ("P01R.tsv", None, "3", "Mińsk Maz. Dworzec Autobusowy", "P01R", "1"),

                ("P02.tsv", None, "4", "Okuniew", "P02", "0"),
                ("P02R.tsv", None, "4", "Stanisławów Rynek", "P02R", "1"),

                ("P03.tsv", None, "5", "Okuniew", "P03", "0"),
                ("P03R.tsv", None, "5", "Michałów Pętla", "P03R", "1"),

                ("P04.tsv", WEEKDAY_CAL_ID, "6", headsign_P04, None, "0"),
                ("P04R.tsv", WEEKDAY_CAL_ID, "6", "Mińsk Maz. Dworzec Autobusowy", None, "1"),

                ("P05.tsv", WEEKDAY_CAL_ID, "7", "Grzebowilk OSP", "P05", "0"),
                ("P05R.tsv", WEEKDAY_CAL_ID, "7", "Mińsk Maz. Dworzec Autobusowy", "P05R", "1"),

                ("P06.tsv", WEEKDAY_CAL_ID, "8", "Cisie Wiadukt", "P06", "0"),
                ("P06R.tsv", WEEKDAY_CAL_ID, "8", "Mińsk Maz. Dworzec Autobusowy", "P06R", "1"),

                ("P07.tsv", None, "9", "Dobre Rynek", "P07", "0"),
                ("P07R.tsv", None, "9", "Mińsk Maz. Dworzec Autobusowy", "P07R", "1"),

                ("P08.tsv", None, "10", headsign_P08, None, "0"),
                ("P08R.tsv", None, "10", "Mińsk Maz. Dworzec Autobusowy", None, "1"),
            ]

            for file in files:
                self.create_trips_from_file(file[0], file[1], file[2], file[3], file[4], file[5], r.db)

    def create_trips_from_file(self, file, calendar, route, headsign, shape, direction, db: DBConnection):
        after_midnight = False
        cal_prev = None
        cal_idx = None
        shape_idx = None
        block_idx = None
        start_idx = 0
        data = (
            pd.read_csv(f"data/{file}", sep="\t", header=None)
            .transpose()
            .values.tolist()
        )
        stops = data[0]
        for idx, stop in enumerate(stops):
            if stop == "shape":
                shape_idx = idx
                start_idx += 1
                continue
            if stop == "cal":
                cal_idx = idx
                start_idx += 1
                continue
            if stop == "block":
                block_idx = idx
                start_idx += 1
                continue
            self.create_stop(int(stop), db)
        for trip in data[1:]:

            trip_id = str(uuid.uuid4())
            calendar_id = trip[cal_idx] if cal_idx is not None else calendar
            shape_id = trip[shape_idx] if shape_idx is not None else shape if shape else None

            if calendar_id != cal_prev:
                after_midnight = False
            cal_prev = calendar_id

            db.create(
                Trip(
                    id=trip_id,
                    route_id=route,
                    calendar_id=calendar_id,
                    short_name=route,
                    headsign=headsign[shape_id] if isinstance(headsign, dict) else headsign,
                    block_id=trip[block_idx] if block_idx is not None else None,
                    shape_id=shape_id,
                    direction=Trip.Direction.INBOUND if direction == "1" else Trip.Direction.OUTBOUND if direction == "0" else None
                )
            )

            for i in range(len(trip)):
                if i < start_idx:
                    continue
                if trip[i] == "~":
                    continue
                if i + 1 < len(trip) and stops[i] == stops[i + 1]:
                    continue

                departure_time: TimePoint = _hour_to_time_point(trip[i])

                # Hack for trips finishing after midnight
                if (
                    i - 1 >= start_idx
                    and trip[i - 1] != "~"
                    and departure_time < _hour_to_time_point(trip[i - 1])
                ) or (after_midnight):
                    after_midnight = True
                    departure_time += TimePoint(hours=24)

                if i - 1 >= 0 and stops[i] == stops[i - 1]:
                    arrival_time = _hour_to_time_point(trip[i - 1])
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


def _hour_to_time_point(time: str) -> TimePoint:
    hour, minute = time.split(":")
    return TimePoint(hours=int(hour), minutes=int(minute))
