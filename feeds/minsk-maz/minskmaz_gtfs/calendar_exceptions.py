import impuls
from impuls import TaskRuntime
from impuls.tools.polish_calendar_exceptions import (
    CalendarExceptionType,
    PolishRegion,
    load_exceptions,
)
from impuls.model import Date, CalendarException
from impuls.tools.temporal import BoundedDateRange
from .consts import WEEKDAY_CAL_ID, SAT_CAL_ID, SUN_CAL_ID, START_DATE, END_DATE


class CalendarExceptions(impuls.Task):
    def __init__(self) -> None:
        super().__init__()
        self.range = BoundedDateRange(
            Date.from_ymd_str(START_DATE), Date.from_ymd_str(END_DATE)
        )

    @staticmethod
    def override_day(r: TaskRuntime, date: str, removed: str, added: str):
        r.db.create(
            CalendarException(
                calendar_id=removed,
                date=date,
                exception_type=CalendarException.Type.REMOVED,
            )
        )
        r.db.create(
            CalendarException(
                calendar_id=added,
                date=date,
                exception_type=CalendarException.Type.ADDED,
            )
        )

    def execute(self, r: TaskRuntime):

        exceptions = load_exceptions(
            r.resources["calendar_exceptions.csv"], PolishRegion.MAZOWIECKIE
        )
        for date, exception in exceptions.items():

            if date not in self.range:
                continue
            if (
                CalendarExceptionType.HOLIDAY not in exception.typ
            ):
                continue

            date_str = str(date)
            day_of_week = date.weekday()

            if day_of_week == 6:
                if CalendarExceptionType.COMMERCIAL_SUNDAY in exception.typ:
                    to_remove = SUN_CAL_ID
                    to_add = SAT_CAL_ID
            elif day_of_week == 5:
                to_remove = SAT_CAL_ID
                to_add = SUN_CAL_ID
            else:
                to_remove = WEEKDAY_CAL_ID
                to_add = SUN_CAL_ID

            r.db.create(
                CalendarException(
                    calendar_id=to_remove,
                    date=date_str,
                    exception_type=CalendarException.Type.REMOVED,
                )
            )
            r.db.create(
                CalendarException(
                    calendar_id=to_add,
                    date=date_str,
                    exception_type=CalendarException.Type.ADDED,
                )
            )

        self.override_day(r, "2025-11-10", WEEKDAY_CAL_ID, SAT_CAL_ID)

        self.override_day(r, "2025-12-14", SUN_CAL_ID, SAT_CAL_ID)
        self.override_day(r, "2025-12-21", SUN_CAL_ID, SAT_CAL_ID)

        # self.override_day(r, "2025-12-24", WEEKDAY_CAL_ID, SUN_CAL_ID)

        self.override_day(r, "2026-01-02", WEEKDAY_CAL_ID, SAT_CAL_ID)
        self.override_day(r, "2026-01-05", WEEKDAY_CAL_ID, SAT_CAL_ID)
