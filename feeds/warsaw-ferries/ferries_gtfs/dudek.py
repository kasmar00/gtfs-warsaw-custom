import impuls
import datetime

from .consts import START_DATE, END_DATE
from .calendar import PUBLIC_HOLIDAYS


class LoadDudek(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            r.db.create(
                impuls.model.Route(
                    id="D",
                    agency_id="2",
                    short_name="D",
                    long_name="Dudek",
                    type=impuls.model.Route.Type.FERRY,
                )
            )

            r.db.create(
                impuls.model.Calendar(
                    id="dudek-week",
                    monday=True,
                    tuesday=True,
                    wednesday=True,
                    thursday=True,
                    friday=True,
                    start_date=START_DATE,
                    end_date="2026-08-31",
                )
            )
            r.db.create(
                impuls.model.Calendar(
                    id="dudek-free",
                    saturday=True,
                    sunday=True,
                    start_date=START_DATE,
                    end_date="2026-08-31",
                )
            )

            for holiday in PUBLIC_HOLIDAYS:
                r.db.create(
                    impuls.model.CalendarException(
                        calendar_id="dudek-free",
                        date=holiday,
                        exception_type=impuls.model.CalendarException.Type.ADDED,
                    )
                )
                r.db.create(
                    impuls.model.CalendarException(
                        calendar_id="dudek-week",
                        date=holiday,
                        exception_type=impuls.model.CalendarException.Type.REMOVED,
                    )
                )

            r.db.create_many(
                impuls.model.Stop,
                [
                    impuls.model.Stop(
                        id="DL",
                        name="Młociny",
                        lat=0,
                        lon=0,
                    ),
                    impuls.model.Stop(
                        id="DR",
                        name="Nowodwory",
                        lat=0,
                        lon=0,
                    ),
                ],
            )

            stop_pairs = {
                "DL": "DR",
                "DR": "DL",
            }

            for calendar, stops in SCHEDULE.items():
                for stop, departures in stops.items():
                    second_stop = stop_pairs[stop]
                    for departure in departures:
                        try:
                            trip_id = f"D:{calendar[6]}:{stop}:{departure}"
                            r.db.create(
                                impuls.model.Trip(
                                    id=trip_id,
                                    route_id="D",
                                    calendar_id=calendar,
                                    bikes_allowed=True,
                                    wheelchair_accessible=True,
                                    shape_id=stop,
                                )
                            )
                            time = impuls.model.TimePoint.from_str(
                                departure + ":00"
                            )
                            second_time = time + datetime.timedelta(minutes=5)
                            r.db.create(
                                impuls.model.StopTime(
                                    stop_id=stop,
                                    trip_id=trip_id,
                                    departure_time=time,
                                    arrival_time=time,
                                    stop_sequence=0,
                                    pickup_type=impuls.model.StopTime.PassengerExchange.SCHEDULED_STOP,
                                    drop_off_type=impuls.model.StopTime.PassengerExchange.NONE,
                                )
                            )
                            r.db.create(
                                impuls.model.StopTime(
                                    stop_id=second_stop,
                                    trip_id=trip_id,
                                    departure_time=second_time,
                                    arrival_time=second_time,
                                    stop_sequence=1,
                                    pickup_type=impuls.model.StopTime.PassengerExchange.NONE,
                                    drop_off_type=impuls.model.StopTime.PassengerExchange.SCHEDULED_STOP,
                                )
                            )
                        except:
                            self.logger.error("Error on adding trip: " + trip_id)



SCHEDULE = {
    "dudek-week": {
        "DL": [
            ":".join([str(h), str(m)])
            for h in [12, 13, 14, 15, 16, 17, 18, 19]
            for m in ["15", "45"]
        ],
        "DR": [
            ":".join([str(h), str(m)])
            for h in [12, 13, 14, 15, 16, 17, 18, 19]
            for m in ["00", "30"]
        ],
    },
    "dudek-free": {
        "DL": [
            ":".join([str(h), str(m)])
            for h in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
            for m in ["10", "30", "50"]
        ],
        "DR": [
            ":".join([str(h), str(m)])
            for h in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
            for m in ["00", "20", "40"]
        ],
    }
}
