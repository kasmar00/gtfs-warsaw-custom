import impuls
import datetime
import argparse
from .consts import START_DATE, END_DATE
from .load_trips import LoadTrips
from .calendar import LoadCalendar
from .shapes import LoadShapes
from .kopernik import LoadKopernik
from .piaseczno import LoadPiaseczno
from .serock import LoadSerock

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
    ),
    "stops.txt": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
    "routes.txt": (
        "agency_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "trips.txt": (
        "route_id",
        "trip_id",
        "service_id",
        "trip_headsign",
        "trip_short_name",
        "shape_id",
        "bikes_allowed",
        "wheelchair_accessible",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "drop_off_type",
        "pickup_type",
    ),
    "calendar_dates.txt": ("service_id", "date", "exception_type"),
    "feed_info.txt": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "default_lang",
        "feed_start_date",
        "feed_end_date",
        "feed_version",
        "feed_contact_email",
        "feed_contact_url",
    ),
    "calendar.txt": (
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ),
    "shapes.txt": (
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
    ),
}

class FerriesGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.AddEntity(
                    impuls.model.Agency(
                        id="1",
                        name="Warszawskie Linie Turystyczne",
                        url="https://www.wtp.waw.pl/warszawskie-linie-turystyczne/promy-przez-wisle/",
                        timezone="Europe/Warsaw",
                        lang="pl",
                    ),
                    task_name="AddAgency",
                ),
                # impuls.tasks.AddEntity(
                #     impuls.model.Agency(
                #         id="2",
                #         name="Prom Dudek",
                #         url="https://www.facebook.com/p/Prom-Dudek-61559135011810/",
                #         timezone="Europe/Warsaw",
                #         lang="pl",
                #     ),
                #     task_name="AddAgency",
                # ),
                impuls.tasks.AddEntity(
                    impuls.model.FeedInfo(
                        publisher_name="kasmar00",
                        publisher_url="https://github.com/kasmar00/gtfs-warsaw-custom",
                        start_date=START_DATE,
                        end_date=END_DATE,
                        lang="pl",
                        version=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                    task_name="AddFeedInfo",
                ),
                LoadShapes(),
                LoadTrips(),
                LoadCalendar(),
                LoadKopernik(),
                LoadPiaseczno(),
                LoadSerock(),
                impuls.tasks.ModifyRoutesFromCSV("routes.csv", must_curate_all=True),
                impuls.tasks.ModifyStopsFromCSV("stops.csv", must_curate_all=True),
                impuls.tasks.GenerateTripHeadsign(),
                impuls.tasks.SaveGTFS(
                    headers=GTFS_HEADERS,
                    target="latest.zip",
                ),
            ],
            resources={
                "routes.csv": impuls.LocalResource("data/routes.txt"),
                "stops.csv": impuls.LocalResource("data/stops.txt"),
            },
            options=options,
        )


def main() -> None:
    FerriesGTFS().run()
