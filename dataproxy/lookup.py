from . import utils
from bookiesports import BookieSports
from dataproxy import Config


class NotNormalizableException(Exception):
    pass


class BookieLookup(object):
    """
        This class serves as the normalization entry point for incidents.
        All events / event group and participant names are replaced with the
        counterpart stored in the bookiesports package.
    """

    ALIAS_TO_SPORT = {}

    LOOKUP = BookieSports(Config.get("bookiesports_chain", default="beatrice"))

    NOT_FOUND = {}

    def _get_sport_identifier(self,
                              sport_name_in_incident,
                              errorIfNotFound=False):
        """
        Tries to find the sport in bookiesports and returns its identifier.

        :param sport_name_in_incident: name given by provider
        :type sport_name_in_incident: str
        :returns the normalized sport name
        """
        for key, sport in self.LOOKUP.items():  # @UnusedVariable
            if self._search_name_and_alias(sport_name_in_incident, sport):
                return sport["identifier"]

        BookieLookup.not_found(sport_name_in_incident)
        if errorIfNotFound:
            raise NotNormalizableException()
        return sport_name_in_incident

    def _search_name_and_alias(self,
                               search_for,
                               search_container):
        if search_container.get("aliases") and utils.search_in(
                search_for,
                search_container["aliases"]):
            return True
        if utils.search_in(search_for, search_container["name"].values()):
            return True
        if search_container.get("identifier", None) == search_for:
            return True
        return False

    def _string_to_date(self, date_string, from_or_to):
        if type(date_string) == str:
            if len(date_string) == len("YYYY/YY/YY"):
                date_string = date_string[0:4] + "-" + date_string[5:7] + "-" + date_string[8:10] + "T"
                if from_or_to == "from":
                    date_string = date_string + "00:00:00Z"
                else:
                    date_string = date_string + "23:59:59Z"
            elif len(date_string) == len("YYYY/YY/YY XX:XX:XX"):
                date_string = date_string[0:4] + "-" + date_string[5:7] + "-" + date_string[8:10] + "T" + date_string[11:19] + "Z"

        if type(date_string).__name__ == "datetime":
            return date_string
        else:
            return utils.string_to_date(date_string)

    def _start_time_within(self, eventgroup, start_date):
        if eventgroup.get("finish_date", None) is None and eventgroup.get("start_date", None) is None:
            return True

        start_date = utils.string_to_date(start_date)
        return start_date <= self._string_to_date(eventgroup["finish_date"], "to") and start_date >= self._string_to_date(eventgroup["start_date"], "from")

    def _get_eventgroup_identifier(self,
                                   sport_identifier,
                                   event_group_name_in_incident,
                                   event_start_time_in_incident,
                                   errorIfNotFound=False):
        """
        Tries to find the eventgroup in bookiesports and returns its identifier.

        :param sport_identifier: name given by provider
        :type sport_identifier: str
        :param event_group_name_in_incident: name given by provider
        :type event_group_name_in_incident: str
        :returns the normalized eventgroup name
        """
        for key, sport in self.LOOKUP.items():  # @UnusedVariable
            if sport["identifier"] == sport_identifier:
                for keyt, valuet in sport["eventgroups"].items():  # @UnusedVariable @IgnorePep8
                    if self._search_name_and_alias(
                            event_group_name_in_incident,
                            valuet) and\
                            self._start_time_within(valuet, event_start_time_in_incident):
                        return valuet["identifier"]

        BookieLookup.not_found(
            sport_identifier + "/" + event_group_name_in_incident)
        if errorIfNotFound:
            raise NotNormalizableException()
        return event_group_name_in_incident

    def _get_participant_identifier(self,
                                    sport_identifier,
                                    event_group_identifier,
                                    participant_name_in_incident,
                                    errorIfNotFound=False):
        """
        Tries to find the participant in bookiesports and returns its identifier.

        :param sport_identifier: name given by provider
        :type sport_identifier: str
        :param event_group_identifier: name given by provider
        :type event_group_identifier: str
        :param participant_name_in_incident: name given by provider
        :type participant_name_in_incident: str
        :returns the participant eventgroup name
        """
        for key, sport in self.LOOKUP.items():  # @UnusedVariable
            if sport["identifier"] == sport_identifier:
                for teamsfile, participants in sport["participants"].items():  # @UnusedVariable @IgnorePep8
                    for participant in participants["participants"]:
                        if self._search_name_and_alias(
                                participant_name_in_incident,
                                participant):
                            try:
                                return participant["identifier"]
                            except KeyError:
                                return participant["name"]["en"]

        BookieLookup.not_found(
            sport_identifier + "/" + event_group_identifier + "/" + participant_name_in_incident)
        if errorIfNotFound:
            raise NotNormalizableException()
        return participant_name_in_incident

    @staticmethod
    def not_found(key):
        BookieLookup.NOT_FOUND[key] = ""

    @staticmethod
    def normalize_incident(incident, errorIfNotFound=False):
        normalized_incident = incident.copy()
        lookup = BookieLookup()
        sport_identifier = lookup._get_sport_identifier(
            incident["id"]["sport"],
            errorIfNotFound=errorIfNotFound)
        event_group_identifier = lookup._get_eventgroup_identifier(
            sport_identifier,
            incident["id"]["event_group_name"],
            incident["id"]["start_time"],
            errorIfNotFound=errorIfNotFound)
        home_identifier = lookup._get_participant_identifier(
            sport_identifier,
            event_group_identifier,
            incident["id"]["home"],
            errorIfNotFound=errorIfNotFound)
        away_identifier = lookup._get_participant_identifier(
            sport_identifier,
            event_group_identifier,
            incident["id"]["away"],
            errorIfNotFound=errorIfNotFound)

        normalized_incident["id"]["sport"] = sport_identifier
        normalized_incident["id"]["event_group_name"] = event_group_identifier
        normalized_incident["id"]["home"] = home_identifier
        normalized_incident["id"]["away"] = away_identifier

        return normalized_incident
