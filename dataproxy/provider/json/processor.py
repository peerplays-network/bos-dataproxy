from datetime import datetime
import pytz
from ...processors import JsonProcessor
from dataproxy import utils


class GenericJsonProcessor(JsonProcessor):

    def __init__(self, correct_timezone=None):
        super(GenericJsonProcessor, self).__init__()
        self._correct_timezone = correct_timezone

    def fix_timezone(self, incident):
        """
        The provider_info may contain the wrong timezone, this allows to adjust it
        :param incident:
        :type incident:
        """
        def convert_string_from_tz(date_string, from_timezone=None):
            if from_timezone is None:
                from_timezone = pytz.timezone(self._correct_timezone)

            if "T" in date_string and "Z" in date_string:
                return date_string
            try:
                target = from_timezone.localize(utils.string_to_date(utils.date_to_string(date_string)))
            except ValueError:
                target = from_timezone.localize(datetime.utcnow())
                target = utils.string_to_date(utils.date_to_string(date_string)).replace(tzinfo=target.tzinfo)

            return utils.date_to_string(target.astimezone(pytz.UTC))

        if incident["provider_info"].get("tzfix", None) is None or not incident["provider_info"]["tzfix"]:
            # convert timezone
            incident["id"]["start_time"] = convert_string_from_tz(incident["id"]["start_time"])

            incident["provider_info"]["pushed"] = convert_string_from_tz(incident["provider_info"]["pushed"])

            if incident["arguments"].get("whistle_start_time", None) is not None:
                incident["arguments"]["whistle_start_time"] = convert_string_from_tz(incident["arguments"]["whistle_start_time"])
            if incident["arguments"].get("whistle_end_time", None) is not None:
                incident["arguments"]["whistle_end_time"] = convert_string_from_tz(incident["arguments"]["whistle_end_time"])

            incident["provider_info"]["tzfix"] = True

        return incident

    def _process_source(self, source, source_type):
        incident = super(GenericJsonProcessor, self)._process_source(source, source_type)
        if self._correct_timezone is None:
            return incident
        else:
            return self.fix_timezone(incident)
