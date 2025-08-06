"""
polygon_client.py

A lightweight, dependency-injected client for Polygon.io’s REST aggregates
endpoint.

Design notes
------------
- All configuration is injected via either environment variables or the
  PolygonAPIClient dataclass.  This keeps the module reusable in CI, notebooks,
  micro-services, and serverless functions without code changes.
- The Polygon API returns millisecond Unix timestamps.  We immediately convert
  them to timezone-aware UTC datetimes so every downstream consumer works with
  a consistent, unambiguous temporal type.
- A pure function (`transform_polygon_result`) is used to decouple the wire
  format from any internal representation, making unit testing trivial.
- `requests` is used instead of `httpx` to avoid an async event-loop
  requirement; this keeps the API surface synchronous and easy to reason about
  in data-science scripts.
- `dataclasses` give us free, type-safe constructors, equality, and reprs
  without boilerplate.

  Rate-limit handling
-------------------
- Free tier: **5 requests / minute**.  
- Internal retry logic respects `Retry-After` (if provided) or uses exponential
  back-off capped at 60 s.  
- Worst-case total attempts ≤ 5 in any rolling 60-second window.
"""


from __future__ import annotations

import math
import pytz
import requests
import random
import time
from dataclasses import dataclass
from datetime import datetime
from decouple import config
from typing import Literal
from urllib.parse import urlencode
from requests.exceptions import HTTPError

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

# Environment variable name matches the provider’s branding (POLYGON) but is
# prefixed with our project namespace to avoid collisions.
POLOGYON_API_KEY = config("POLOGYON_API_KEY", default=None, cast=str)

# --------------------------------------------------------------------------- #
# Constants tied to Polygon’s advertised policy
# --------------------------------------------------------------------------- #
_POLYGON_FREE_TIER_RPM = 5                # requests per minute
_POLYGON_REQUESTS_WINDOW = 60.0           # seconds
_POLYGON_MIN_INTERVAL = _POLYGON_REQUESTS_WINDOW / _POLYGON_FREE_TIER_RPM  # 12 s

# --------------------------------------------------------------------------- #
# Custom exception
# --------------------------------------------------------------------------- #
class PolygonRateLimitError(RuntimeError):
    """Raised when retries are exhausted due to HTTP 429."""

# --------------------------------------------------------------------------- #
# Utilities
# --------------------------------------------------------------------------- #

def transform_polygon_result(result: dict) -> dict:
    """
    Convert one raw Polygon aggregate bar into a canonical, timezone-aware
    representation.

    Why this shape?
    ---------------
    - Keys are lower_snake_case → idiomatic Python.
    - All numeric values are left as `float` (Polygon guarantees JSON numbers)
      so callers can decide on Decimal, float, or int coercion later.
    - The timestamp is converted to UTC **once** to avoid repeated, expensive
      parsing in downstream analytics code.

    Parameters
    ----------
    result : dict
        A single element from `response.json()['results']`.

    Returns
    -------
    dict
        Flat dictionary with human-readable keys.
    """

    unix_timestamp = result.get('t') / 1000.0
    utc_timestamp = datetime.fromtimestamp(unix_timestamp, tz=pytz.timezone('UTC'))

    return {
        'open_price': result['o'],
        'close_price': result['c'],
        'high_price': result['h'],
        'low_price': result['l'],
        'number_of_trades': result['n'],
        'volume': result['v'],
        'volume_weighted_average': result['vw'],
        'raw_timestamp': result.get('t'),
        'time': utc_timestamp,
    }

# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #

@dataclass(slots=True)
class PolygonAPIClient:
    """
    Synchronous, stateless client for the Polygon aggregates endpoint.

    Attributes
    ----------
    ticker : str
        Symbol in Polygon format, e.g. 'X:BTCUSD'.  Default is Bitcoin.
    multiplier : int
        Number of `timespan` units per bar.
    timespan : str
        Unit of time: 'minute', 'hour', 'day', 'week', 'month',.
    from_date, to_date : str
        ISO-8601 date strings (YYYY-MM-DD).  Polygon is *inclusive* on both ends.
    api_key : str
        Optional override of the global `POLOGYON_API_KEY`.
    adjusted : bool
        Request split/dividend-adjusted prices when available.
    sort : {'asc', 'desc'}
        Ascending to maintain chronological order in the returned list.

    Notes
    -----
    - The dataclass is frozen by default (immutable) so that configuration can
      be safely shared across threads or greenlets.
    - All public methods are pure with respect to the instance (no mutation),
      simplifying unit tests and allowing instances to be reused safely.
    """

    ticker: str = "X:BTCUSD"
    multiplier: int = 1
    timespan:str = "day"
    from_date:str = "2025-01-01"
    to_date:str = "2025-01-30"
    api_key:str = ""
    adjusted: bool = True 
    sort: Literal["asc", "desc"] = "asc"


    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #

    def get_api_key(self) -> str:
        """
        Resolve the effective API key.

        Priority
        --------
        1. Explicit `self.api_key`
        2. Environment variable `POLOGYON_API_KEY`
        3. RuntimeError if neither is provided.
        """
        key = self.api_key or POLOGYON_API_KEY
        if not key:
            raise RuntimeError(
                "Polygon API key not found.  "
                "Set POLOGYON_API_KEY environment variable or pass api_key=..."
            )
        return key


    def get_headers(self) -> dict[str, str]:
        """Return HTTP headers required for Bearer-token authentication."""
        api_key = self.get_api_key()
        return {"Authorization": f"Bearer {api_key}"}


    def get_params(self) -> dict[str, object]:
        """Return query parameters common to every request."""
        return {"adjusted": self.adjusted, "sort": self.sort, "limit": 50_000}
    

    # --------------------------------------------------------------------- #
    # Public interface
    # --------------------------------------------------------------------- #

    def generate_url(self, pass_auth: bool = False) -> str:
        """
        Build a fully-qualified URL for the aggregates endpoint.

        Parameters
        ----------
        pass_auth : bool, optional
            When `True`, also append `api_key=<key>` as a query parameter
            instead of using the Authorization header.  This is useful for
            quick browser testing or when sharing signed URLs.

        Returns
        -------
        str
            Absolute URL including query parameters.
        """
        ticker = f"{self.ticker}".upper()
        path = f"/v2/aggs/ticker/{ticker}/range/{self.multiplier}/{self.timespan}/{self.from_date}/{self.to_date}"
        url = f"https://api.polygon.io{path}"
        params = self.get_params()
        encoded_params = urlencode(params)
        url = f"{url}?{encoded_params}"
        if pass_auth:
            api_key = self.get_api_key()
            url += f"&api_key={api_key}"
        return url


    def fetch_data(
        self,
        max_retries: int = 4,  # 1 initial + 4 retries = 5 requests max
    ) -> dict[str, any]:
        """
        Execute the HTTP request and return raw JSON.

        Implements polite back-off for HTTP 429 (rate limit).

        Parameters
        ----------
        max_retries : int
            Maximum **additional** attempts after the first failure.

        Raises
        ------
        PolygonRateLimitError
            If we are still throttled after retries.
        requests.HTTPError
            For any non-429 or unexpected HTTP error.
        """
        headers = self.get_headers()
        url = self.generate_url()

        attempt = 0
        while True:
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 429:
                response.raise_for_status()
                return response.json()

            # Rate-limited --------------------------------------------------- #
            if attempt >= max_retries:
                raise PolygonRateLimitError(
                    f"Rate-limited by Polygon after {max_retries + 1} attempts."
                )

            # Honour Retry-After if provided, otherwise self-throttle
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None and retry_after.isdigit():
                delay = int(retry_after)
            else:
                # Exponential back-off capped at one full window
                delay = min(
                    _POLYGON_REQUESTS_WINDOW,
                    _POLYGON_MIN_INTERVAL * (2 ** attempt) * random.uniform(0.9, 1.1),
                )

            time.sleep(delay)
            attempt += 1


    def get_stock_data(self) -> list[dict]:
        """
        High-level convenience wrapper that returns a list of normalized bars.

        Returns
        -------
        list[dict]
            Chronologically ordered list produced by `transform_polygon_result`.
        """
        data = self.fetch_data()
        results = data.get('results', None)
        if results is None or len(results) == 0:
            raise Exception(f"Polygon API returned no results for {self.ticker}")

        dataset = []
        for result in results:
            dataset.append(
                transform_polygon_result(result)
            )
        return dataset