import datetime

import impuls
from .schedules import DEPARTURES

from .consts import START_DATE, END_DATE


class LoadTrips(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            r.db.create(
                impuls.model.Calendar(
                    id="weekday",
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
                impuls.model.Calendar(
                    id="freeday",
                    saturday=True,
                    sunday=True,
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            )

            stops_pairs = {
                "W1R": "W1L",
                "W1L": "W1R",
                "W2L": "W2R",
                "W2R": "W2L",
                "W3L": "W3R",
                "W3R": "W3L",
            }

            for stop in stops_pairs.keys():
                r.db.create(
                    impuls.model.Stop(
                        id=stop,
                        name=stop,
                        lat=0,
                        lon=0,
                    )
                )

            for line, calendars in DEPARTURES.items():
                r.db.create(
                    impuls.model.Route(
                        line, 1, line, line, impuls.model.Route.Type.FERRY
                    )
                )
                for calendar, stops in calendars.items():
                    for stop, departures in stops.items():
                        second_stop = stops_pairs[stop]
                        for departure in departures:
                            try:
                                trip_id = f"{line}:{calendar[0]}:{stop}:{departure}"
                                r.db.create(
                                    impuls.model.Trip(
                                        id=trip_id,
                                        route_id=line,
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
