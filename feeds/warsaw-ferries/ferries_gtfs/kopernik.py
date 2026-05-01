import impuls
import datetime

from .consts import START_DATE, END_DATE


class LoadKopernik(impuls.Task):
    def execute(self, r):
        r.db.create(impuls.model.Route("K", 1, "K", "K", impuls.model.Route.Type.FERRY))
        r.db.create(
            impuls.model.Stop(
                id="GK",
                name="GK",
                lat=0,
                lon=0,
            )
        )

        r.db.create(
            impuls.model.Calendar(
                id="kopernik",
                thursday=True,
                friday=True,
                saturday=True,
                sunday=True,
                start_date=START_DATE,
                end_date=END_DATE,
            )
        )
        r.db.create(
            impuls.model.CalendarException(
                calendar_id="kopernik",
                date=datetime.date(2026, 6, 1),
                exception_type=impuls.model.CalendarException.Type.ADDED,
            )
        )

        for i in ["12:00", "13:40", "15:20"]:
            trip_id = f"K:{i}"
            r.db.create(
                impuls.model.Trip(
                    id=trip_id,
                    route_id="K",
                    calendar_id="kopernik",
                    bikes_allowed=False,
                    wheelchair_accessible=True,
                )
            )
            time = impuls.model.TimePoint.from_str(i + ":00")
            r.db.create(
                impuls.model.StopTime(
                    stop_id="W3L",
                    trip_id=trip_id,
                    departure_time=time,
                    arrival_time=time,
                    stop_sequence=0,
                    pickup_type=impuls.model.StopTime.PassengerExchange.SCHEDULED_STOP,
                    drop_off_type=impuls.model.StopTime.PassengerExchange.NONE,
                )
            )
            second_time = time + datetime.timedelta(minutes=45)

            r.db.create(
                impuls.model.StopTime(
                    stop_id="GK",
                    trip_id=trip_id,
                    departure_time=second_time,
                    arrival_time=second_time,
                    stop_sequence=1,
                    pickup_type=impuls.model.StopTime.PassengerExchange.NONE,
                    drop_off_type=impuls.model.StopTime.PassengerExchange.NONE,
                )
            )

            third_time = second_time + datetime.timedelta(minutes=45)
            r.db.create(
                impuls.model.StopTime(
                    stop_id="W3L",
                    trip_id=trip_id,
                    departure_time=third_time,
                    arrival_time=third_time,
                    stop_sequence=2,
                    pickup_type=impuls.model.StopTime.PassengerExchange.NONE,
                    drop_off_type=impuls.model.StopTime.PassengerExchange.SCHEDULED_STOP,
                )
            )
