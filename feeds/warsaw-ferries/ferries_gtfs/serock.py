import impuls
import datetime

from .consts import START_DATE, END_DATE
from .calendar import PUBLIC_HOLIDAYS


class LoadSerock(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            r.db.create(
                impuls.model.Route(
                    id="S",
                    agency_id="1",
                    short_name="S",
                    long_name="Serock",
                    type=impuls.model.Route.Type.FERRY,
                )
            )

            r.db.create(
                impuls.model.Calendar(
                    id="serock",
                    saturday=True,
                    sunday=True,
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            )

            for holiday in PUBLIC_HOLIDAYS:
                r.db.create(
                    impuls.model.CalendarException(
                        calendar_id="serock",
                        date=holiday,
                        exception_type=impuls.model.CalendarException.Type.ADDED,
                    )
                )

            r.db.create_many(
                impuls.model.Stop,
                [
                    impuls.model.Stop(
                        id="Cementownia",
                        name="Cementownia",
                        lat=0,
                        lon=0,
                    ),
                    impuls.model.Stop(
                        id="Serock",
                        name="Serock",
                        lat=0,
                        lon=0,
                    ),
                ]
            )

            r.db.create_many(
                impuls.model.Trip,
                [
                    impuls.model.Trip(
                        id="S-do",
                        route_id="S",
                        calendar_id="serock",
                        bikes_allowed=False,
                        wheelchair_accessible=True,
                    ),
                    impuls.model.Trip(
                        id="S-z",
                        route_id="S",
                        calendar_id="serock",
                        bikes_allowed=False,
                        wheelchair_accessible=True,
                    ),
                ]
            )

            r.db.create_many(
                impuls.model.StopTime,
                [
                    impuls.model.StopTime(
                        trip_id="S-do",
                        stop_id="Cementownia",
                        arrival_time=impuls.model.TimePoint.from_str("09:00:00"),
                        departure_time=impuls.model.TimePoint.from_str("09:00:00"),
                        stop_sequence=1,
                    ),
                    impuls.model.StopTime(
                        trip_id="S-do",
                        stop_id="Serock",
                        arrival_time=impuls.model.TimePoint.from_str("12:30:00"),
                        departure_time=impuls.model.TimePoint.from_str("12:30:00"),
                        stop_sequence=2,
                    ),
                    impuls.model.StopTime(
                        trip_id="S-z",
                        stop_id="Serock",
                        arrival_time=impuls.model.TimePoint.from_str("14:30:00"),
                        departure_time=impuls.model.TimePoint.from_str("14:30:00"),
                        stop_sequence=1,
                    ),
                    impuls.model.StopTime(
                        trip_id="S-z",
                        stop_id="Cementownia",
                        arrival_time=impuls.model.TimePoint.from_str("18:00:00"),
                        departure_time=impuls.model.TimePoint.from_str("18:00:00"),
                        stop_sequence=2,
                    ),
                ]
            )
