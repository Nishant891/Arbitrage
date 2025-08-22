from typing import Iterable, Generator
import time
import requests
from itertools import chain

try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda *args, **kwargs: args[0]

BASE_URL = "api.the-odds-api.com/v4"
PROTOCOL = "https://"


class APIException(RuntimeError):
    def __str__(self):
        return f"('{self.args[0]}', '{self.args[1].json()['message']}')"


class AuthenticationException(APIException):
    pass


class RateLimitException(APIException):
    pass


def handle_faulty_response(response: requests.Response):
    if response.status_code == 401:
        raise AuthenticationException("Failed to authenticate with the API. is the API key valid?", response)
    elif response.status_code == 429:
        raise RateLimitException("Encountered API rate limit.", response)
    else:
        raise APIException("Unknown issue arose while trying to access the API.", response)

def get_sports(key: str) -> set[str]:
    # Gets all the sports using the endpoint https://api.the-odds-api.com/v4/sports/?apiKey
    # Loops through the array and fetches eachb items[key] = sport
    # [
    #   {"key":"americanfootball_cfl","group":"American Football","title":"CFL","description":"Canadian Football League","active":true,"has_outrights":false},
    #   {"key":"americanfootball_ncaaf","group":"American Football","title":"NCAAF","description":"US College Football","active":true,"has_outrights":false}
    # ]
    # Returns a set of sport keys (like "soccer_epl", "basketball_nba")
    # All the regions have same sports but, different boookmakers
    url = f"{BASE_URL}/sports/"
    escaped_url = PROTOCOL + requests.utils.quote(url)
    querystring = {"apiKey": key}

    response = requests.get(escaped_url, params=querystring)
    if not response:
        handle_faulty_response(response)

    return {item["key"] for item in response.json()}


def get_data(key: str, sport: str, region: str = "eu"):
    # For every sport in sports we want to get the odds from the bookmakers of a specific region
    # THis is returned when the below api is called, this get's us for each game between two teams all the bookmakers and their odds
#   [
#   {
#     "id": "f1bc532dff946d15cb85654b5c4b246e",
#     "sport_key": "americanfootball_nfl",
#     "sport_title": "NFL",
#     "commence_time": "2025-09-05T00:21:00Z",
#     "home_team": "Philadelphia Eagles",
#     "away_team": "Dallas Cowboys",
#     "bookmakers": [
#       {
#         "key": "draftkings",
#         "title": "DraftKings",
#         "last_update": "2025-07-24T17:50:10Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:50:09Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.6
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.31
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "key": "lowvig",
#         "title": "LowVig.ag",
#         "last_update": "2025-07-24T17:50:37Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:50:37Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.3
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.36
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "key": "betonlineag",
#         "title": "BetOnline.ag",
#         "last_update": "2025-07-24T17:50:37Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:50:37Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.3
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.36
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "key": "mybookieag",
#         "title": "MyBookie.ag",
#         "last_update": "2025-07-24T17:48:39Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:48:39Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.44
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.3
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "key": "betmgm",
#         "title": "BetMGM",
#         "last_update": "2025-07-24T17:50:37Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:50:37Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.6
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.31
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "key": "fanduel",
#         "title": "FanDuel",
#         "last_update": "2025-07-24T17:50:38Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:50:38Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.65
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.3
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "key": "betrivers",
#         "title": "BetRivers",
#         "last_update": "2025-07-24T17:50:40Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:50:40Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.4
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.32
#               }
#             ]
#           }
#         ]
#       },
#       {
#         "key": "betus",
#         "title": "BetUS",
#         "last_update": "2025-07-24T17:50:22Z",
#         "markets": [
#           {
#             "key": "h2h",
#             "last_update": "2025-07-24T17:50:21Z",
#             "outcomes": [
#               {
#                 "name": "Dallas Cowboys",
#                 "price": 3.35
#               },
#               {
#                 "name": "Philadelphia Eagles",
#                 "price": 1.36
#               }
#             ]
#           }
#         ]
#       }
#     ]
#   }
# ]
    url = f"{BASE_URL}/sports/{sport}/odds/"
    escaped_url = PROTOCOL + requests.utils.quote(url)
    querystring = {
        "apiKey": key,
        "regions": region,
        "oddsFormat": "decimal",
        "dateFormat": "unix"
    }

    response = requests.get(escaped_url, params=querystring)
    if not response:
        handle_faulty_response(response)

    return response.json()


def process_data(matches: Iterable, include_started_matches: bool = True) -> Generator[dict, None, None]:
    """Extracts all matches that are available and calculates some things about them, such as the time to start and
    the best available implied odds."""
    matches = tqdm(matches, desc="Checking all matches", leave=False, unit=" matches")
    for match in matches:
        start_time = int(match["commence_time"])
        # Once the match starts bookmakers stop taking bets and we cannot find arbitrage
        if not include_started_matches and start_time < time.time():
            continue
        # This is the core logic we are trying to find the best odd for each team so for the team 1 if the bookmaker's odd > previous bookmaker's odd keep it
        # Same for team 2 if the bookmaker's odd > previous bookmaker's odd keep it 
        # Why are we trying to maximize the price for each team through out the bookmaker's
        # To calculate the arbitrage we want to divide those maximum values by 1 which gives us the minimum value and the sum of these values < 1
        # So the sum of all the odds i.e 1/max(price) team1 + 1/max(price) team2 < 1 then my friend we have an arbitrage opportunity 
        best_odd_per_outcome = {}
        for bookmaker in match["bookmakers"]:
            bookie_name = bookmaker["title"]
            for outcome in bookmaker["markets"][0]["outcomes"]:
                outcome_name = outcome["name"]
                odd = outcome["price"]
                if outcome_name not in best_odd_per_outcome.keys() or \
                    odd > best_odd_per_outcome[outcome_name][1]:
                    best_odd_per_outcome[outcome_name] = (bookie_name, odd)

        total_implied_odds = sum(1/i[1] for i in best_odd_per_outcome.values())
        match_name = f"{match['home_team']} v. {match['away_team']}"
        time_to_start = (start_time - time.time())/3600
        league = match["sport_key"]
        yield {
            "match_name": match_name,
            "match_start_time": start_time,
            "hours_to_start": time_to_start,
            "league": league,
            "best_outcome_odds": best_odd_per_outcome,
            "total_implied_odds": total_implied_odds,
        }


def get_arbitrage_opportunities(key: str, region: str, cutoff: float):
    sports = get_sports(key)
    data = chain.from_iterable(get_data(key, sport, region=region) for sport in sports)
    data = filter(lambda x: x != "message", data)
    results = process_data(data)
    arbitrage_opportunities = filter(lambda x: 0 < x["total_implied_odds"] < 1-cutoff, results)

    return arbitrage_opportunities
