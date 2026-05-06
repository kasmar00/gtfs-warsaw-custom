import impuls
import datetime

from .consts import START_DATE, END_DATE


class LoadPiaseczno(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            r.db.create(
                impuls.model.Route(
                    id="51",
                    agency_id="1",
                    short_name="51",
                    long_name="51",
                    type=impuls.model.Route.Type.BUS,
                )
            )

            r.db.create(
                impuls.model.Route(
                    id="KP",
                    agency_id="1",
                    short_name="KP",
                    long_name="KP",
                    type=impuls.model.Route.Type.RAIL,
                )
            )

            for stop_id in [
                "KP-DS01",
                "KP-DS02",
                "KP-MW01",
                "KP-MW02",
                "KP-PK01",
                "KP-PK03",
                "KP-P",
                "KP-R",
                "KP-T",
            ]:
                r.db.create(
                    impuls.model.Stop(
                        id=stop_id,
                        name=stop_id,
                        lat=0,
                        lon=0,
                    )
                )

            r.db.create(
                impuls.model.Calendar(
                    id="piaseczno",
                    saturday=True,
                    start_date=START_DATE,
                    end_date=END_DATE,
                )
            )

            r.db.create(
                impuls.model.CalendarException(
                    calendar_id="piaseczno",
                    date=datetime.date(2026, 5, 2),
                    exception_type=impuls.model.CalendarException.Type.ADDED,
                )
            )

            for trip_id in ["KP-1", "KP-2"]:
                r.db.create(
                    impuls.model.Trip(
                        id=trip_id,
                        route_id="KP",
                        calendar_id="piaseczno",
                    )
                )

            for trip_id in ["51-DO", "51-Z"]:
                r.db.create(
                    impuls.model.Trip(
                        id=trip_id,
                        route_id="51",
                        calendar_id="piaseczno",
                    )
                )

            for stop_time in [
                ("51-DO", 1, "KP-DS01", "13:00:00"),
                ("51-DO", 2, "KP-MW01", "13:10:00"),
                ("51-DO", 3, "KP-PK01", "13:45:00"),
                ("51-Z", 1, "KP-PK03", "18:15:00"),
                ("51-Z", 2, "KP-MW02", "18:30:00"),
                ("51-Z", 3, "KP-DS02", "18:45:00"),
            ]:
                r.db.create(
                    impuls.model.StopTime(
                        trip_id=stop_time[0],
                        stop_sequence=stop_time[1],
                        stop_id=stop_time[2],
                        arrival_time=impuls.model.TimePoint.from_str(stop_time[3]),
                        departure_time=impuls.model.TimePoint.from_str(stop_time[3]),
                    )
                )


            for stop_time in [
                ("KP-1", 1, "KP-P", "14:00:00", "14:00:00"),
                ("KP-1", 2, "KP-T", "14:30:00", "14:45:00"),
                ("KP-1", 3, "KP-R", "15:00:00", "15:00:00"),
                ("KP-2", 1, "KP-R", "17:15:00", "17:15:00"),
                ("KP-2", 2, "KP-P", "17:40:00", "17:40:00"),
            ]:
                r.db.create(
                    impuls.model.StopTime(
                        trip_id=stop_time[0],
                        stop_sequence=stop_time[1],
                        stop_id=stop_time[2],
                        arrival_time=impuls.model.TimePoint.from_str(stop_time[3]),
                        departure_time=impuls.model.TimePoint.from_str(stop_time[4]),
                    )
                )
