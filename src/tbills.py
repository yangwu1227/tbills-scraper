import io
import math
from datetime import date, datetime, timezone
from re import match
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, TypedDict, Union

import awswrangler as wr
import boto3
import pandas as pd
import polars as pl
import polars.selectors as cs
import requests
import sympy as sp
from loguru import logger
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Stats(TypedDict):
    """
    Typed structure for insert/merge statistics.
    """

    rows_processed: int
    rows_inserted: int
    rows_updated: int


class AWSSettings(BaseSettings):
    """
    AWS settings loaded from environment variables or a `.env` file. When
    the S3 tables catalog is integrated with Data Catalog and Lake Formation, the
    AWS Glue service creates a single catalog called `s3tablescatalog`:

    - Amazon S3 table buckets become a multi-level catalog in the Data Catalog
    - The associated Amazon S3 namespace is registered as a database in the Data Catalog
    - The Amazon S3 tables in the table bucket becomes tables in the Data Catalog

    This allows us to query S3 tables via Athena.

    Attributes
    ----------
    table_name : str
        Target Iceberg table name in the S3 table bucket namespace.
        Env var: `TABLE_NAME`.
    athena_workgroup : str
        Athena workgroup to use. Env var: `ATHENA_WORKGROUP` (default: `primary`).
    athena_output_s3 : str
        S3 uri for Athena query results (must start with `s3://`).
        Env var: `ATHENA_OUTPUT_S3`.
    catalog : str
        Data catalog name (`s3tablescatalog`, or `s3tablescatalog/<namespace>`, or `AwsDataCatalog`).
        Env var: `CATALOG`.
    database : str
        Database/namespace within the catalog. Env var: `DATABASE`.
    aws_region : str
        AWS region name. Env var: `AWS_REGION` (default: `us-east-1`).

    Notes
    -----
    Settings are read from environment and optionally a `.env` file in the current working directory.
    """

    table_name: str = Field(..., validation_alias="TABLE_NAME")
    athena_workgroup: str = Field(
        default="primary", validation_alias="ATHENA_WORKGROUP"
    )
    athena_output_s3: str = Field(..., validation_alias="ATHENA_OUTPUT_S3")
    catalog: str = Field(..., validation_alias="CATALOG")
    database: str = Field(..., validation_alias="DATABASE")
    aws_region: str = Field(default="us-east-1", validation_alias="AWS_REGION")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
    )

    @field_validator("athena_output_s3")
    @classmethod
    def _validate_s3_uri(cls, v: str) -> str:
        """
        Validate and normalize the Athena output S3 uri.

        Parameters
        ----------
        v : str
            S3 URI string.

        Returns
        -------
        str
            Normalized S3 uri ending with a trailing slash.

        Raises
        ------
        ValueError
            If the uri does not start with `s3://`.
        """
        if not v.startswith("s3://"):
            raise ValueError("ATHENA_OUTPUT_S3 must start with 's3://'")
        return v if v.endswith("/") else v + "/"

    @field_validator("catalog")
    @classmethod
    def _validate_catalog(cls, v: str) -> str:
        """
        Validate the catalog identifier.

        Parameters
        ----------
        v : str
            Catalog identifier.

        Returns
        -------
        str
            The provided catalog string if valid.

        Raises
        ------
        ValueError
            If the catalog string is not one of the supported values.
        """
        v_stripped: str = v.strip()
        v_lower: str = v_stripped.lower()
        if v_lower == "awsdatacatalog" or v_lower == "s3tablescatalog":
            return v_stripped
        if v_lower.startswith("s3tablescatalog/"):
            name_spaces: str = v_stripped.split("/", 1)[1]
            if name_spaces and match(r"^[A-Za-z0-9_-]+$", name_spaces):
                return v_stripped
        raise ValueError(
            "CATALOG must be 'AwsDataCatalog', 's3tablescatalog', or 's3tablescatalog/<namespace>'."
        )

    @field_validator("database", "table_name")
    @classmethod
    def _validate_identifiers(cls, v: str) -> str:
        """
        Lightly validate identifiers for database and table names.

        Parameters
        ----------
        v : str
            Identifier string.

        Returns
        -------
        str
            The provided identifier if valid.

        Raises
        ------
        ValueError
            If the identifier contains characters outside `[a-zA-Z0-9_]`.
        """
        if not match(r"^[a-zA-Z0-9_]+$", v):
            raise ValueError(f"Identifier contains invalid chars: {v!r}")
        return v


class TreasuryBillScraper(object):
    """
    Scraper that upserts Treasury bill yields into an Iceberg table
    in Amazon S3 Tables via Athena (engine v3) using `awswrangler`.

    Parameters
    ----------
    settings : Optional[AWSSettings]
        Injected settings instance. If not provided, a fresh `AWSSettings` is created.

    Attributes
    ----------
    settings : AWSSettings
        Loaded AWS settings.
    table_name : str
        Target table name.
    session : boto3.session.Session
        Boto3 session bound to `settings.aws_region`.
    """

    def __init__(
        self,
        settings: Optional[AWSSettings] = None,
        boto3_session: Optional[boto3.session.Session] = None,
    ) -> None:
        """
        Initialize the scraper with validated settings.

        Parameters
        ----------
        settings : Optional[AWSSettings]
            Injected settings instance. If not provided, a fresh `AWSSettings` is created.
        boto3_session : Optional[boto3.session.Session]
            Injected Boto3 session. If not provided, a new session is created.

        Returns
        -------
        None
        """
        self.settings: AWSSettings = settings or AWSSettings()
        self.table_name: str = self.settings.table_name
        self.boto3_session: boto3.session.Session = boto3_session or boto3.Session(
            region_name=self.settings.aws_region
        )

    def scrape_treasury_data(self) -> pl.DataFrame:
        """
        Fetch the latest treasury daily treasury bill bond-equivalent yields.

        Returns
        -------
        pl.DataFrame
            DataFrame (long-format) containing the latest treasury yields
            for each maturity bucket: 4, 6, 8, 13, 17, 26, 52 weeks.

        Raises
        ------
        requests.HTTPError
            If the HTTP request failed.
        """
        year_month: str = datetime.now().strftime("%Y%m")
        url: str = (
            "https://home.treasury.gov/"
            "resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/"
            f"all/{year_month}"
            f"?type=daily_treasury_bill_rates"
            f"&field_tdr_date_value_month={year_month}"
        )

        logger.info(f"Fetching data from {url}")
        response: requests.Response = requests.get(url, timeout=30)
        response.raise_for_status()

        data: pl.DataFrame = (
            pl.scan_csv(
                io.BytesIO(response.content),
            )
            .select(cs.contains("COUPON EQUIVALENT") | cs.by_name("Date"))
            .with_columns(pl.col("Date").str.strptime(dtype=pl.Date, format="%m/%d/%Y"))
            .filter(pl.col("Date") == pl.col("Date").max())
            .select(pl.all().name.to_lowercase())
            .unpivot(
                index="date",
                on=cs.contains("weeks"),
                variable_name="maturity",
                value_name="yield_pct",
            )
            .with_columns(
                [
                    pl.col("maturity")
                    .str.replace_all(r"\D+", "")
                    .cast(pl.Int16)
                    .alias("maturity"),
                    pl.col("yield_pct").cast(pl.Float32).alias("yield_pct"),
                    pl.lit(datetime.now(timezone.utc)).alias("scrape_timestamp"),
                ]
            )
        ).collect()
        logger.info(f"Treasury bills yields successfully scraped: {data.shape[0]} rows")

        return data

    @staticmethod
    def _format_python_value(value: Any) -> str:
        """
        Format a Python value as a SQL literal.

        Parameters
        ----------
        value : Any
            Value to format.

        Returns
        -------
        str
            SQL literal representation.
        """
        if value is None:
            return "NULL"

        if isinstance(value, (float, int)):
            return str(value)

        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            value_utc = value.astimezone(timezone.utc)
            ts = value_utc.strftime("%Y-%m-%d %H:%M:%S.%f")
            return f"TIMESTAMP '{ts} UTC'"

        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"

        s: str = str(value).replace("'", "''")
        return f"'{s}'"

    @classmethod
    def _values_clause(cls, rows: Iterable[Tuple[date, int, float, datetime]]) -> str:
        """
        Build a VALUES clause for from typed row tuples.

        Parameters
        ----------
        rows : Iterable[tuple[datetime.date, int, float, datetime]]
            Iterable of rows in the order: (date, maturity, yield_pct, scrape_timestamp).

        Returns
        -------
        str
            Comma-separated VALUES list suitable for inline `USING (VALUES ...)`.
        """
        parts: List[str] = []
        for d, m, y, ts in rows:
            parts.append(
                f"({cls._format_python_value(d)},"
                f"{cls._format_python_value(m)},"
                f"{cls._format_python_value(y)},"
                f"{cls._format_python_value(ts)})"
            )
        return ",\n".join(parts)

    def _execute_query(self, query: str) -> str:
        """
        Execute a query and wait for completion.

        Parameters
        ----------
        query : str
            SQL DML to execute.

        Returns
        -------
        str
            Athena QueryExecutionId.
        """
        qid: str = wr.athena.start_query_execution(
            sql=query,
            database=self.settings.database,
            s3_output=self.settings.athena_output_s3,
            workgroup=self.settings.athena_workgroup,
            data_source=self.settings.catalog,
            boto3_session=self.boto3_session,
            wait=False,
        )
        wr.athena.wait_query(qid, boto3_session=self.boto3_session)
        return qid

    def _select_query(self, query: str) -> pd.DataFrame:
        """
        Execute an Athena SELECT query and return the result as a DataFrame.

        Parameters
        ----------
        query : str
            SQL SELECT query to execute.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the query results.
        """
        data: pd.DataFrame = wr.athena.read_sql_query(
            sql=query,
            database=self.settings.database,
            workgroup=self.settings.athena_workgroup,
            s3_output=self.settings.athena_output_s3,
            data_source=self.settings.catalog,
            ctas_approach=False,
            boto3_session=self.boto3_session,
        )
        return data

    def upsert_data(self, data: pl.DataFrame) -> Stats:
        """
        Upsert into an Iceberg table using `MERGE INTO` (engine v3).

        The logical key is `(date, maturity)`, where existing rows are
        updated only when `new_data.scrape_timestamp > target.scrape_timestamp`.

        Parameters
        ----------
        data : pl.DataFrame
            Input records with columns: `date`, `maturity`, `yield_pct`, `scrape_timestamp`.

        Returns
        -------
        Stats
            Counts for processed, inserted, and updated rows.

        Raises
        ------
        ValueError
            If required columns are missing.
        """
        stats: Stats = {
            "rows_processed": len(data),
            "rows_inserted": 0,
            "rows_updated": 0,
        }
        if len(data) == 0:
            logger.info("No new data to upsert")
            return stats

        required_cols: List[str] = ["date", "maturity", "yield_pct", "scrape_timestamp"]
        missing_cols: List[str] = [
            col for col in required_cols if col not in data.columns
        ]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        rows: List[Tuple[date, int, float, datetime]] = list(
            zip(
                data["date"].to_list(),
                data["maturity"].to_list(),
                data["yield_pct"].to_list(),
                data["scrape_timestamp"].to_list(),
            )
        )

        values_clause: str = self._values_clause(rows)

        # Use a two-part identifier when `data_source` is provided
        table_identifier: str = f'"{self.settings.database}"."{self.table_name}"'

        to_insert_count_query: str = f"""
            SELECT 
                COUNT(*) AS count
            FROM 
                (VALUES
                    {values_clause}
                ) AS source(date, maturity, yield_pct, scrape_timestamp)
            LEFT JOIN 
                {table_identifier} AS target
            ON
                1 = 1
                AND target.date = source.date
                AND target.maturity = source.maturity
            WHERE 
                target.date IS NULL;
        """

        to_update_count_query: str = f"""
            SELECT 
                COUNT(*) AS count
            FROM 
                (VALUES
                    {values_clause}
                ) AS source(date, maturity, yield_pct, scrape_timestamp)
            INNER JOIN 
                {table_identifier} AS target
            ON 
                1 = 1
                AND target.date = source.date
            AND target.maturity = source.maturity
            WHERE 
                source.scrape_timestamp > target.scrape_timestamp;
        """

        to_insert_count_data: pd.DataFrame = self._select_query(to_insert_count_query)
        to_update_count_data: pd.DataFrame = self._select_query(to_update_count_query)
        stats["rows_inserted"] = int(to_insert_count_data["count"].at[0])
        stats["rows_updated"] = int(to_update_count_data["count"].at[0])
        logger.info(f"Upsert stats: {stats}")

        upsert_query: str = f"""
            MERGE INTO 
                {table_identifier} AS target
            USING 
                (VALUES
                    {values_clause}
                ) AS source(date, maturity, yield_pct, scrape_timestamp)
            ON 
                target.date = source.date
                AND target.maturity = source.maturity
            WHEN NOT MATCHED THEN
                INSERT (date, maturity, yield_pct, scrape_timestamp)
                VALUES (source.date, source.maturity, source.yield_pct, source.scrape_timestamp)
            WHEN MATCHED AND source.scrape_timestamp > target.scrape_timestamp THEN
                UPDATE SET 
                    yield_pct = source.yield_pct,
                    scrape_timestamp = source.scrape_timestamp;
        """

        _: str = self._execute_query(upsert_query)

        return stats


class TreasuryBillAnalytics(object):
    """
    Analytics for Treasury-bill roll-vs-roll decisions under an integer-roll
    plus self-consistent stub convention.

    The class compares a "short" tenor (rolled at an unknown break-even
    coupon-equivalent yield, CEY, denoted `y_be`) against a "long" tenor
    (rolled at its observed CEY), over a common horizon in days. The short side
    uses a self-consistent stub: any non-integer leftover days are also invested
    at the same `y_be` being solved for.

    Terminology
    -----------
    CEY
        Coupon-equivalent yield, expressed as an annualized rate on a
        `day_count_base` basis (e.g., 365).
    Short tenor
        The shorter maturity (in weeks) for the Treasury bill.
    Long tenor
        The longer maturity (in weeks) for the Treasury bill.
    Stub
        The leftover days after the maximum integer number of full rolls that
        fit within the horizon.

    Attributes
    ----------
    data : pl.DataFrame
        Input table.
    cey_by_weeks : Dict[int, float]
        Mapping from tenor in weeks to CEY as a decimal (e.g., 0.0432).
    available_tenors_weeks : List[int]
        Sorted list of available tenors (weeks).
    """

    _y: sp.Symbol = sp.symbols("y", real=True)  # Unknown break-even CEY (decimal)
    _dc: sp.Symbol = sp.symbols("dc", positive=True)  # Day-count base (symbolic)

    def __init__(
        self,
        input_table: pl.DataFrame,
        day_count_base: int = 365,
        days_per_week: int = 7,
    ) -> None:
        """
        Initialize the analytics object and precompute convenience mappings.

        Parameters
        ----------
        input_table : pl.DataFrame
            Must include `maturity` (weeks) and `yield_pct` (percent CEY).
        day_count_base : int, optional
            Day-count base for CEY scaling (default `365`).
        days_per_week : int, optional
            Days per week for tenor conversion (default `7`).

        Raises
        ------
        ValueError
            If required columns are missing.
        """
        self.data: pl.DataFrame = input_table
        self.day_count_base: int = day_count_base
        self.days_per_week: int = days_per_week

        required_columns: Set[str] = {"maturity", "yield_pct"}
        missing: Set[str] = required_columns - set(self.data.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Map tenor (weeks) -> coupon-equivalent yield (decimal)
        self.cey_by_weeks: Dict[int, float] = dict(
            zip(
                self.data["maturity"].to_list(),
                (
                    self.data.select((pl.col("yield_pct") / 100.0).alias("yield_dec"))
                    .get_column("yield_dec")
                    .to_list()
                ),
            )
        )
        self.available_tenors_weeks: List[int] = sorted(self.cey_by_weeks.keys())

        return None

    def _decompose_horizon_into_full_rolls_and_stub(
        self, horizon_days: int, tenor_days: int
    ) -> Tuple[int, int]:
        """
        Decompose a horizon into the number of full tenor rolls plus a stub.

        Parameters
        ----------
        horizon_days : int
            Investment horizon in days (e.g., 365).
        tenor_days : int
            Tenor length in days (e.g., 28 for 4 weeks).

        Returns
        -------
        full_rolls : int
            Number of full tenor rolls that fit into the horizon.
        stub_days : int
            Remaining days after the full rolls (`horizon_days - full_rolls * tenor_days`).
        """
        full_rolls: int = horizon_days // tenor_days
        stub_days: int = horizon_days - full_rolls * tenor_days
        return full_rolls, stub_days

    def _sympy_accumulation_factor_constant_cey(
        self,
        y_symbolic: Union[sp.Symbol, sp.Float],
        tenor_days: int,
        horizon_days: int,
    ) -> sp.Expr:
        """
        Build the SymPy expression for the accumulation factor under integer rolls + stub:

        `(1 + y * m / dc)^k * (1 + y * r / dc)`

        where (k, r) is the integer decomposition of `horizon_days` by `tenor_days`.

        Parameters
        ----------
        y_symbolic : sympy.Symbol or sympy.Float
            Yield variable or numeric CEY (decimal).
        tenor_days : int
            Tenor length in days.
        horizon_days : int
            Common investment horizon in days.

        Returns
        -------
        sympy.Expr
            Symbolic accumulation-factor expression (no numeric evaluation).
        """
        m_int: sp.Integer = sp.Integer(tenor_days)

        k_val, r_val = self._decompose_horizon_into_full_rolls_and_stub(
            horizon_days, tenor_days
        )
        k_int: sp.Integer = sp.Integer(k_val)
        r_int: sp.Integer = sp.Integer(r_val)

        expr: sp.Expr = (1 + y_symbolic * m_int / self._dc) ** k_int * (
            1 + y_symbolic * r_int / self._dc
        )
        expr = expr.subs({self._dc: sp.Integer(self.day_count_base)})
        return expr

    def _sympy_rhs_accumulation_from_long_leg(
        self,
        y_long_decimal: float,
        long_tenor_days: int,
        horizon_days: int,
    ) -> sp.Expr:
        """
        Construct the SymPy expression for the long leg's accumulation to the horizon:

        `(1 + y_long (m_long / dc)) ** k_long * (1 + y_long (r_long / dc))`

        Parameters
        ----------
        y_long_decimal : float
            Observed CEY for the long tenor, decimal (e.g., `0.0413`).
        long_tenor_days : int
            Long tenor length in days.
        horizon_days : int
            Common investment horizon in days.

        Returns
        -------
        sympy.Expr
            Symbolic expression (with numeric atoms) for the RHS accumulation.
        """
        y_long_num: sp.Float = sp.Float(y_long_decimal)
        rhs_expr: sp.Expr = self._sympy_accumulation_factor_constant_cey(
            y_symbolic=y_long_num,
            tenor_days=long_tenor_days,
            horizon_days=horizon_days,
        )
        return rhs_expr

    def _solve_y_be_self_consistent_against_accumulation_level(
        self,
        target_accumulation_level: float,
        short_tenor_days: int,
        horizon_days: int,
    ) -> float:
        """
        Solve for `y_be` (decimal) in:

        `(1 + y_{be} (m_s / dc)) ** k_s * (1 + y_{be} (r_s / dc)) = {target_accumulation_level}`

        with `(k_s, r_s)` defined by decomposing `horizon_days` by
        `short_tenor_days`.

        If `r_s = 0`, a closed-form solution is used. Otherwise, the scalar
        equation is solved numerically using `sympy.nsolve`, seeded by a
        fractional-roll shortcut.

        Parameters
        ----------
        target_accumulation_level : float
            Gross return factor on the right-hand side (e.g., the "long" tenor
            compounded to the horizon).
        short_tenor_days : int
            Short tenor length in days (e.g., 28 for 4 weeks).
        horizon_days : int
            Common investment horizon in days.

        Returns
        -------
        float
            Break-even coupon-equivalent yield (decimal). Returns `nan` if no
            feasible solution is found.

        Notes
        -----
        Feasibility requires positive period growth: `1 + y * m / dc > 0` for
        both the short full-roll period and the short stub.
        """
        dc_int: sp.Integer = sp.Integer(self.day_count_base)
        m_s_int: sp.Integer = sp.Integer(short_tenor_days)
        H_int: sp.Integer = sp.Integer(horizon_days)

        k_s_val: int
        r_s_val: int
        k_s_val, r_s_val = self._decompose_horizon_into_full_rolls_and_stub(
            int(H_int), int(m_s_int)
        )
        if k_s_val <= 0:
            return float("nan")

        k_s_int: sp.Integer = sp.Integer(k_s_val)
        r_s_int: sp.Integer = sp.Integer(r_s_val)

        # Exact symbolic equation f(y) = 0
        lhs_expr: sp.Expr = (1 + self._y * m_s_int / self._dc) ** k_s_int * (
            1 + self._y * r_s_int / self._dc
        )
        lhs_expr = lhs_expr.subs({self._dc: sp.Integer(self.day_count_base)})

        target_expr: sp.Float = sp.Float(target_accumulation_level)
        f_expr: sp.Expr = sp.expand(lhs_expr - target_expr)

        # Case A: no short stub, use closed-form solution
        if r_s_val == 0:
            eq_closed_form: sp.Eq = sp.Eq(
                (1 + self._y * m_s_int / dc_int) ** k_s_int, target_expr
            )
            solutions: sp.Expr = sp.solve(eq_closed_form, self._y)
            if not solutions:
                return float("nan")
            y_be_closed_expr: sp.Expr = sp.simplify(solutions[0])
            y_closed: float = float(y_be_closed_expr)

            return y_closed

        # Case B: stub present, use nsolve with fractional-roll seed
        initial_guess_expr: sp.Expr = sp.simplify(
            ((target_expr) ** (sp.Rational(m_s_int, H_int)) - 1) * dc_int / m_s_int
        )
        initial_guess: float = float(initial_guess_expr)

        # Try seeds around the initial guess, plus/minus 0.02 (200 basis points or 2 percentage points)
        for seed in (initial_guess, initial_guess - 0.02, initial_guess + 0.02):
            try:
                root_sym: sp.Float = sp.nsolve(
                    f_expr,
                    self._y,
                    sp.Float(seed),
                    tol=sp.Float(1e-16),
                    maxsteps=200,
                    prec=80,
                )
                root_val: float = float(root_sym)
                # Feasibility checks
                if (1 + root_val * float(m_s_int) / float(dc_int)) > 0.0 and (
                    1 + root_val * float(r_s_int) / float(dc_int)
                ) > 0.0:
                    return root_val
            except Exception as error:
                logger.debug(f"Using nsolve failed with seed {seed}: {error}")
                continue

        return float("nan")

    def compute_break_even_rates(
        self,
        decimals: int,
    ) -> pl.DataFrame:
        """
        Compute the break-even CEY (percent) for each ordered pair
        (`shorter_maturity_weeks`, `longer_maturity_weeks`), rolling both to
        a common horizon. The short side uses a self-consistent stub at `y_be`;
        the long side compounds at its observed CEY.

        Parameters
        ----------
        decimals : int
            Decimal places to round the break-even implied forward yield (%).

        Returns
        -------
        pl.DataFrame
            Long-form table with columns:
                - `Shorter Maturity (weeks)`
                - `Shorter CEY (%)`
                - `Longer Maturity (weeks)`
                - `Longer CEY (%)`
                - `Break-Even CEY (%)`
        """
        results: List[Dict[str, Union[int, float]]] = []

        logger.info(
            f"Computing break-even table with horizon = {self.day_count_base} days"
        )
        for longer_weeks in self.available_tenors_weeks:
            m_long_days: int = int(longer_weeks) * self.days_per_week
            if m_long_days > self.day_count_base:
                continue

            y_long_dec: float = self.cey_by_weeks[longer_weeks]
            rhs_accum_expr: sp.Expr = self._sympy_rhs_accumulation_from_long_leg(
                y_long_decimal=y_long_dec,
                long_tenor_days=m_long_days,
                horizon_days=self.day_count_base,
            )
            rhs_accum_val: float = float(rhs_accum_expr)

            for shorter_weeks in self.available_tenors_weeks:
                if shorter_weeks >= longer_weeks:
                    continue
                m_short_days: int = int(shorter_weeks) * self.days_per_week
                if m_short_days > self.day_count_base:
                    continue

                y_be_dec: float = (
                    self._solve_y_be_self_consistent_against_accumulation_level(
                        target_accumulation_level=rhs_accum_val,
                        short_tenor_days=m_short_days,
                        horizon_days=self.day_count_base,
                    )
                )
                if math.isfinite(y_be_dec):
                    results.append(
                        {
                            "Shorter Maturity (weeks)": int(shorter_weeks),
                            "Shorter CEY (%)": round(
                                100.0 * self.cey_by_weeks[shorter_weeks], decimals
                            ),
                            "Longer Maturity (weeks)": int(longer_weeks),
                            "Longer CEY (%)": round(
                                100.0 * self.cey_by_weeks[longer_weeks], decimals
                            ),
                            "Break-Even Implied Forward Yield (%)": round(
                                100.0 * y_be_dec, decimals
                            ),
                        }
                    )

        y_be_table: pl.DataFrame = pl.from_dicts(results).sort(
            ["Longer Maturity (weeks)", "Shorter Maturity (weeks)"]
        )
        logger.info(f"Break-even table computed: {y_be_table.shape[0]} rows")
        return y_be_table
