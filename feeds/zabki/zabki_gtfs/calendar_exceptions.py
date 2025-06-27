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

    def execute(self, r: TaskRuntime):

        exceptions = load_exceptions(
            r.resources["calendar_exceptions.csv"], PolishRegion.MAZOWIECKIE
        )
        for date, exception in exceptions.items():

            if date not in self.range:
                continue
            if (
                CalendarExceptionType.COMMERCIAL_SUNDAY not in exception.typ
                and CalendarExceptionType.HOLIDAY not in exception.typ
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
