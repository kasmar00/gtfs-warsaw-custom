import impuls

PUBLIC_HOLIDAYS = [
    "2026-05-01",
    "2026-06-04",
    "2026-08-15",
]

class LoadCalendar(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            for holiday in PUBLIC_HOLIDAYS:
                r.db.create(
                    impuls.model.CalendarException(
                        exception_type=impuls.model.CalendarException.Type.ADDED,
                        date = holiday,
                        calendar_id="freeday"
                    )
                )
                r.db.create(
                    impuls.model.CalendarException(
                        exception_type=impuls.model.CalendarException.Type.REMOVED,
                        date = holiday,
                        calendar_id="weekday"
                    )
                )