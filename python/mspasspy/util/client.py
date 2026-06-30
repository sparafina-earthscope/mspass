"""
Gateway-aware drop-in replacement for mspasspy.client.Client.

This preserves the original MsPASS Client public API (get_scheduler,
get_database, get_database_client, get_global_history_manager,
set_database_client, set_global_history_manager, set_scheduler) and the
original constructor-parameter / environment-variable / default resolution
priority. It ADDS a Dask Gateway connection path so MsPASS can run against a
Gateway-provisioned, per-user, Kubernetes cluster (e.g. EarthScope GeoLab)
instead of only a bare LocalCluster or a raw tcp:// scheduler address.

Backwards compatibility:
  * If no Gateway is configured/available, behavior is identical to upstream:
      - scheduler_host / MSPASS_SCHEDULER_ADDRESS  -> DaskClient("host:port")
      - neither set                                -> DaskClient()  (LocalCluster)
  * The MongoDBWorker plugin is still auto-registered on the resulting client.

Gateway activation (dask scheduler only) happens when EITHER:
  * use_gateway=True is passed explicitly, OR
  * use_gateway is None (default) AND the environment looks like Gateway
    (DASK_GATEWAY__ADDRESS present) AND no explicit raw scheduler address was
    requested via scheduler_host / MSPASS_SCHEDULER_ADDRESS.

New constructor parameters (all optional, all keyword-friendly):
  use_gateway      : True / False / None(auto-detect). Default None.
  gateway_address  : override DASK_GATEWAY__ADDRESS.
  gateway_proxy_address : override DASK_GATEWAY__PROXY_ADDRESS.
  gateway_auth     : override auth type (default reads DASK_GATEWAY__AUTH__TYPE,
                     falls back to "jupyterhub").
  cluster_options  : dict of options passed to gateway.new_cluster(**options).
  adapt_minimum    : adaptive scaling floor (default 0).
  adapt_maximum    : adaptive scaling ceiling (default 8).
  reuse_existing   : reconnect to this user's existing Gateway cluster if one
                     exists instead of creating a new one (default True).
"""

import os
import pymongo

from mspasspy.db.client import DBClient
from mspasspy.util.db_utils import MongoDBWorker
from mspasspy.db.database import Database
from mspasspy.global_history.manager import GlobalHistoryManager

try:
    from pyspark import SparkConf, SparkContext

    _mspasspy_has_pyspark = True
except ImportError:
    _mspasspy_has_pyspark = False

try:
    from pyspark.sql import SparkSession
except ImportError:
    _mspasspy_has_pyspark = False

try:
    from dask.distributed import Client as DaskClient

    _mspasspy_has_dask_distributed = True
except ImportError:
    _mspasspy_has_dask_distributed = False

try:
    from dask_gateway import Gateway

    _mspasspy_has_dask_gateway = True
except ImportError:
    _mspasspy_has_dask_gateway = False

from mspasspy.ccore.utility import MsPASSError


class Client:
    """
    A client-side representation of MSPASS (Gateway-aware).

    This is the only client users should use in MSPASS. The client manages all
    the other clients or instances. It creates and manages a Database client, a
    Global History Manager, and a scheduler (spark/dask). When a Dask Gateway is
    configured in the environment, the dask scheduler is obtained from a
    per-user Gateway cluster rather than a LocalCluster.

    For the address and port of each client/instance we first check the
    user-specified parameters, then environment variable values, then the
    default settings.
    """

    def __init__(
        self,
        database_host=None,
        scheduler=None,
        scheduler_host=None,
        job_name="mspass",
        database_name="mspass",
        schema=None,
        collection=None,
        # ---- new Gateway-related keyword args (all optional) ----
        use_gateway=None,
        gateway_address=None,
        gateway_proxy_address=None,
        gateway_auth=None,
        cluster_options=None,
        adapt_minimum=0,
        adapt_maximum=8,
        reuse_existing=True,
    ):
        # ---- original validation (unchanged) ----
        if database_host is not None and not type(database_host) is str:
            raise MsPASSError(
                "database_host should be a string but "
                + str(type(database_host))
                + " is found.",
                "Fatal",
            )
        if scheduler is not None and scheduler != "dask" and scheduler != "spark":
            raise MsPASSError(
                "scheduler should be either dask or spark but "
                + str(scheduler)
                + " is found.",
                "Fatal",
            )
        if scheduler_host is not None and not type(scheduler_host) is str:
            raise MsPASSError(
                "scheduler_host should be a string but "
                + str(type(scheduler_host))
                + " is found.",
                "Fatal",
            )
        if job_name is not None and not type(job_name) is str:
            raise MsPASSError(
                "job_name should be a string but " + str(type(job_name)) + " is found.",
                "Fatal",
            )
        if database_name is not None and not type(database_name) is str:
            raise MsPASSError(
                "database_name should be a string but "
                + str(type(database_name))
                + " is found.",
                "Fatal",
            )
        if collection is not None and type(collection) is not str:
            raise MsPASSError(
                "collection should be a string but "
                + str(type(collection))
                + " is found.",
                "Fatal",
            )

        # ---- stash Gateway-related settings for later use ----
        self._use_gateway = use_gateway
        self._gateway_address = gateway_address
        self._gateway_proxy_address = gateway_proxy_address
        self._gateway_auth = gateway_auth
        self._cluster_options = cluster_options or {}
        self._adapt_minimum = adapt_minimum
        self._adapt_maximum = adapt_maximum
        self._reuse_existing = reuse_existing
        # Handles populated only on the Gateway path; kept for teardown/inspection.
        self._gateway = None
        self._gateway_cluster = None

        # ---- check env variables (unchanged) ----
        MSPASS_DB_ADDRESS = os.environ.get("MSPASS_DB_ADDRESS")
        MONGODB_PORT = os.environ.get("MONGODB_PORT")
        MSPASS_SCHEDULER = os.environ.get("MSPASS_SCHEDULER")
        MSPASS_SCHEDULER_ADDRESS = os.environ.get("MSPASS_SCHEDULER_ADDRESS")
        DASK_SCHEDULER_PORT = os.environ.get("DASK_SCHEDULER_PORT")
        SPARK_MASTER_PORT = os.environ.get("SPARK_MASTER_PORT")

        # ---- database client (unchanged) ----
        database_host_has_port = False
        if database_host:
            database_address = database_host
            if ":" in database_address:
                database_host_has_port = True
        elif MSPASS_DB_ADDRESS:
            database_address = MSPASS_DB_ADDRESS
        else:
            database_address = "127.0.0.1"
        if not database_host_has_port and MONGODB_PORT:
            database_address += ":" + MONGODB_PORT

        try:
            self._db_client = DBClient(database_address)
            self._db_client.server_info()
        except Exception as err:
            raise MsPASSError(
                "Runntime error: cannot create a database client with: "
                + database_address,
                "Fatal",
            )

        # ---- defaults + global history manager (unchanged) ----
        self._default_database_name = database_name
        self._default_schema = schema
        self._default_collection = collection

        if schema:
            global_history_manager_db = Database(
                self._db_client, database_name, db_schema=schema
            )
        else:
            global_history_manager_db = Database(self._db_client, database_name)
        self._global_history_manager = GlobalHistoryManager(
            global_history_manager_db, job_name, collection=collection
        )

        # ---- choose scheduler type (unchanged priority) ----
        if scheduler:
            self._scheduler = scheduler
        elif MSPASS_SCHEDULER:
            self._scheduler = MSPASS_SCHEDULER
        else:
            if _mspasspy_has_dask_distributed:
                self._scheduler = "dask"
            elif _mspasspy_has_pyspark:
                self._scheduler = "spark"
            else:
                self._scheduler = None

        # ---- spark path (unchanged) ----
        if self._scheduler == "spark":
            scheduler_host_has_port = False
            if scheduler_host:
                self._spark_master_url = scheduler_host
                if "spark://" not in scheduler_host:
                    self._spark_master_url = "spark://" + self._spark_master_url
                if self._spark_master_url.count(":") == 2:
                    scheduler_host_has_port = True
            elif MSPASS_SCHEDULER_ADDRESS:
                self._spark_master_url = MSPASS_SCHEDULER_ADDRESS
                if "spark://" not in MSPASS_SCHEDULER_ADDRESS:
                    self._spark_master_url = "spark://" + self._spark_master_url
            else:
                self._spark_master_url = "local"

            if (
                (scheduler_host or MSPASS_SCHEDULER_ADDRESS)
                and not scheduler_host_has_port
                and SPARK_MASTER_PORT
            ):
                self._spark_master_url += ":" + SPARK_MASTER_PORT

            try:
                spark = (
                    SparkSession.builder.appName("mspass")
                    .master(self._spark_master_url)
                    .getOrCreate()
                )
                self._spark_context = spark.sparkContext
            except Exception as err:
                raise MsPASSError(
                    "Runntime error: cannot create a spark configuration with: "
                    + self._spark_master_url,
                    "Fatal",
                )

        elif self._scheduler == "dask":
            # Decide whether to use Gateway. An explicit raw address (param or
            # env) always wins and takes the original tcp:// path, preserving
            # backward compatibility.
            explicit_raw_address = bool(scheduler_host or MSPASS_SCHEDULER_ADDRESS)
            gateway_wanted = self._resolve_use_gateway(explicit_raw_address)

            if gateway_wanted:
                # ---- NEW: Dask Gateway path ----
                self._dask_client = self._connect_via_gateway()
            elif not explicit_raw_address:
                # ---- LocalCluster fallback ----
                # Guard against the silent-fallback bug: if a Gateway
                # environment is clearly present but we somehow reached here,
                # do NOT quietly start an in-pod LocalCluster (the source of
                # the "Port 8787 already in use" symptom). Fail loudly so the
                # misconfiguration is visible, unless the caller explicitly
                # opted out of Gateway via use_gateway=False.
                gateway_env_present = any(
                    k.startswith("DASK_GATEWAY__") for k in os.environ
                ) or bool(self._gateway_address)
                if gateway_env_present and self._use_gateway is not False:
                    raise MsPASSError(
                        "A Dask Gateway environment was detected (DASK_GATEWAY__* "
                        "present) but the Gateway path was not taken. Refusing to "
                        "silently start an in-pod LocalCluster. If you intended to "
                        "use Gateway, ensure the dask_gateway package is installed "
                        "and pass use_gateway=True. If you really want a local "
                        "cluster, pass use_gateway=False explicitly.",
                        "Fatal",
                    )
                self._dask_client = DaskClient()
            else:
                # ---- original raw tcp:// path ----
                scheduler_host_has_port = False
                if scheduler_host:
                    self._dask_client_address = scheduler_host
                    if ":" in scheduler_host:
                        scheduler_host_has_port = True
                else:
                    self._dask_client_address = MSPASS_SCHEDULER_ADDRESS

                if not scheduler_host_has_port and DASK_SCHEDULER_PORT:
                    self._dask_client_address += ":" + DASK_SCHEDULER_PORT
                else:
                    self._dask_client_address += ":8786"
                try:
                    self._dask_client = DaskClient(self._dask_client_address)
                except Exception as err:
                    raise MsPASSError(
                        "Runntime error: cannot create a dask client with: "
                        + self._dask_client_address,
                        "Fatal",
                    )
        else:
            print("There is no spark or dask installed, this client has no scheduler")

        # ---- auto-register MongoDB worker plugin (unchanged) ----
        if self._scheduler == "dask":
            mongo_plugin = MongoDBWorker(self, dbclient_key="dbclient")
            self._dask_client.register_plugin(mongo_plugin, name="mongodb_worker")

    # ------------------------------------------------------------------ #
    # Gateway helpers (new)
    # ------------------------------------------------------------------ #
    def _resolve_use_gateway(self, explicit_raw_address):
        """Decide whether the dask scheduler should come from Dask Gateway.

        Rules:
          * use_gateway=True  -> require Gateway (error if unavailable).
          * use_gateway=False -> never use Gateway.
          * use_gateway=None  -> auto: use Gateway iff dask_gateway is importable,
            a Gateway address is configured (param or DASK_GATEWAY__ADDRESS), and
            the caller did NOT request an explicit raw tcp scheduler address.
        """
        # Detect Gateway from the environment robustly: any DASK_GATEWAY__*
        # variable indicates a Gateway-configured pod (GeoLab injects several,
        # not just __ADDRESS). An explicit override param also counts.
        gateway_env_vars = [k for k in os.environ if k.startswith("DASK_GATEWAY__")]
        addr_configured = bool(
            self._gateway_address
            or os.environ.get("DASK_GATEWAY__ADDRESS")
            or gateway_env_vars
        )

        if self._use_gateway is True:
            if not _mspasspy_has_dask_gateway:
                raise MsPASSError(
                    "use_gateway=True but the dask_gateway package is not installed.",
                    "Fatal",
                )
            if not addr_configured:
                raise MsPASSError(
                    "use_gateway=True but no Gateway address is configured "
                    "(set gateway_address or DASK_GATEWAY__ADDRESS).",
                    "Fatal",
                )
            return True

        if self._use_gateway is False:
            return False

        # auto-detect (use_gateway is None)
        decision = (
            _mspasspy_has_dask_gateway
            and addr_configured
            and not explicit_raw_address
        )

        # Diagnostics: if a Gateway environment is present but we are NOT going
        # to use it, say loudly why -- this is exactly the case that silently
        # produced an in-pod LocalCluster (the "Port 8787 already in use"
        # symptom) instead of connecting to Gateway.
        if addr_configured and not decision:
            reasons = []
            if not _mspasspy_has_dask_gateway:
                reasons.append("dask_gateway package not importable")
            if explicit_raw_address:
                reasons.append(
                    "an explicit raw scheduler address was set "
                    "(scheduler_host / MSPASS_SCHEDULER_ADDRESS), which overrides Gateway"
                )
            import warnings

            warnings.warn(
                "MsPASS Client: a Dask Gateway environment was detected "
                "(DASK_GATEWAY__* present) but Gateway will NOT be used because: "
                + "; ".join(reasons or ["unknown"])
                + ". This would fall back to an in-pod LocalCluster. "
                "Pass use_gateway=True to force Gateway, or unset the raw "
                "scheduler address to enable auto-detection.",
                stacklevel=2,
            )
        return decision

    def _connect_via_gateway(self):
        """Create or reuse a per-user Gateway cluster and return its DaskClient.

        Address, proxy address, auth type, worker image and worker environment
        are read from the DASK_GATEWAY__* environment variables by dask_gateway
        unless overridden via constructor parameters. On GeoLab these env vars
        are injected into the singleuser pod, so Gateway() with no args resolves
        correctly; we pass overrides only when provided.
        """
        gw_kwargs = {}
        if self._gateway_address:
            gw_kwargs["address"] = self._gateway_address
        if self._gateway_proxy_address:
            gw_kwargs["proxy_address"] = self._gateway_proxy_address

        # Auth: default to the env-configured type (jupyterhub on GeoLab).
        auth = self._gateway_auth or os.environ.get(
            "DASK_GATEWAY__AUTH__TYPE", "jupyterhub"
        )
        if auth:
            gw_kwargs["auth"] = auth

        try:
            self._gateway = Gateway(**gw_kwargs)
        except Exception as err:
            raise MsPASSError(
                "Runntime error: cannot create a Dask Gateway handle: " + str(err),
                "Fatal",
            )

        # Reconnect to an existing per-user cluster if asked and one exists.
        cluster = None
        try:
            if self._reuse_existing:
                existing = self._gateway.list_clusters()
                if existing:
                    cluster = self._gateway.connect(existing[0].name)
            if cluster is None:
                cluster = self._gateway.new_cluster(**self._cluster_options)
        except Exception as err:
            raise MsPASSError(
                "Runntime error: cannot create or connect to a Gateway cluster: "
                + str(err),
                "Fatal",
            )

        self._gateway_cluster = cluster

        # Adaptive scaling: minimum=0 keeps idle clusters cheap; maximum is
        # clamped server-side by the Gateway ClusterConfig limits.
        try:
            cluster.adapt(minimum=self._adapt_minimum, maximum=self._adapt_maximum)
        except Exception:
            # Non-fatal: a cluster without adaptive support can still be used.
            pass

        try:
            return DaskClient(cluster)
        except Exception as err:
            raise MsPASSError(
                "Runntime error: cannot create a dask client from the Gateway "
                "cluster: " + str(err),
                "Fatal",
            )

    def get_gateway_cluster(self):
        """Return the underlying Gateway cluster object, or None if not using
        Gateway. Useful for scaling, logs (cluster.shutdown(), etc.)."""
        return self._gateway_cluster

    # ------------------------------------------------------------------ #
    # Original API (unchanged)
    # ------------------------------------------------------------------ #
    def get_database_client(self):
        """
        Get the database client in the global history manager

        :return: :class:`mspasspy.db.database.Database`
        """
        return self._db_client

    def get_database(self, database_name=None):
        """
        Get a database by database_name, if database_name is not specified, use the default one

        :param database_name: the name of database
        :type database_name: :class:`str`
        :return: :class:`mspasspy.db.database.Database`
        """
        if not database_name:
            return Database(self._db_client, self._default_database_name)
        return Database(self._db_client, database_name)

    def get_global_history_manager(self):
        """
        Get the global history manager with this client

        :return: :class:`mspasspy.global_history.manager.GlobalHistoryManager`
        """
        return self._global_history_manager

    def get_scheduler(self):
        """
        Get the scheduler(spark/dask) with this client

        :return: :class:`pyspark.SparkContext`/:class:`dask.distributed.Client`/None
        """
        if self._scheduler == "spark":
            return self._spark_context
        elif self._scheduler == "dask":
            return self._dask_client
        else:
            print(
                "There is no spark or dask installed, this client has no scheduler, returned None"
            )
            return None

    def set_database_client(self, database_host, database_port=None):
        """
        Set a database client by database_host(and database_port)

        :param database_host: the host address of database client
        :type database_host: :class:`str`
        :param database_port: the port of database client
        :type database_port: :class:`str`
        """
        database_host_has_port = False
        database_address = database_host
        if ":" in database_host:
            database_host_has_port = True
        if not database_host_has_port and database_port:
            database_address += ":" + database_port
        temp_db_client = self._db_client
        try:
            self._db_client = DBClient(database_address)
            self._db_client.server_info()
        except Exception as err:
            self._db_client = temp_db_client
            raise MsPASSError(
                "Runntime error: cannot create a database client with: "
                + database_address,
                "Fatal",
            )

    def set_global_history_manager(self, history_db, job_name, collection=None):
        """
        Set a global history manager by history_db, job_name(and collection)

        :param history_db: the database will be set in the global history manager
        :type history_db: :class:`mspasspy.db.database.Database`
        :param job_name: the job name will be set in the global history manager
        :type job_name: :class:`str`
        :param collection: the collection name will be set in the history_db
        :type collection: :class:`str`
        """
        if not isinstance(history_db, Database):
            raise TypeError(
                "history_db should be a mspasspy.db.Database but "
                + str(type(history_db))
                + " is found."
            )
        if not type(job_name) is str:
            raise TypeError(
                "job_name should be a string but " + str(type(job_name)) + " is found."
            )
        if collection is not None and type(collection) is not str:
            raise TypeError(
                "collection should be a string but "
                + str(type(collection))
                + " is found."
            )

        self._global_history_manager = GlobalHistoryManager(
            history_db, job_name, collection=collection
        )

    def set_scheduler(self, scheduler, scheduler_host=None, scheduler_port=None):
        """
        Set a scheduler by scheduler type, scheduler_host(and scheduler_port).

        Extended for Gateway: if scheduler == "dask" and scheduler_host is None
        or the literal "gateway", a Dask Gateway cluster is created/reused using
        the same environment-driven configuration as the constructor.

        :param scheduler: the scheduler type, should be either dask or spark
        :type scheduler: :class:`str`
        :param scheduler_host: the host address of scheduler, or None/"gateway"
            to use Dask Gateway for a dask scheduler
        :type scheduler_host: :class:`str`
        :param scheduler_port: the port of scheduler
        :type scheduler_port: :class:`str`
        """
        if scheduler != "dask" and scheduler != "spark":
            raise MsPASSError(
                "scheduler should be either dask or spark but "
                + str(scheduler)
                + " is found.",
                "Fatal",
            )

        prev_scheduler = self._scheduler
        self._scheduler = scheduler
        if scheduler == "spark":
            if scheduler_host is None:
                raise MsPASSError(
                    "set_scheduler: scheduler_host is required for spark.",
                    "Fatal",
                )
            scheduler_host_has_port = False
            self._spark_master_url = scheduler_host
            if "spark://" not in scheduler_host:
                self._spark_master_url = "spark://" + self._spark_master_url
            if self._spark_master_url.count(":") == 2:
                scheduler_host_has_port = True
            if not scheduler_host_has_port and scheduler_port:
                self._spark_master_url += ":" + scheduler_port

            prev_spark_context = None
            prev_spark_conf = None
            if hasattr(self, "_spark_context"):
                prev_spark_context = self._spark_context
                prev_spark_conf = self._spark_context.getConf()
            try:
                if hasattr(self, "_spark_context") and isinstance(
                    self._spark_context, SparkContext
                ):
                    spark_conf = self._spark_context._conf.setMaster(
                        self._spark_master_url
                    )
                else:
                    spark_conf = (
                        SparkConf()
                        .setAppName("mspass")
                        .setMaster(self._spark_master_url)
                    )
                spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
                self._spark_context = spark.sparkContext
            except Exception as err:
                if prev_spark_conf:
                    self._spark_context = SparkContext.getOrCreate(conf=prev_spark_conf)
                if self._scheduler == "spark" and prev_scheduler == "dask":
                    self._scheduler = prev_scheduler
                raise MsPASSError(
                    "Runntime error: cannot create a spark configuration with: "
                    + self._spark_master_url,
                    "Fatal",
                )
            if hasattr(self, "_dask_client"):
                del self._dask_client

        elif scheduler == "dask":
            prev_dask_client = None
            if hasattr(self, "_dask_client"):
                prev_dask_client = self._dask_client

            # Gateway path: scheduler_host omitted or explicitly "gateway".
            use_gw = scheduler_host is None or scheduler_host == "gateway"
            try:
                if use_gw:
                    self._dask_client = self._connect_via_gateway()
                else:
                    scheduler_host_has_port = False
                    self._dask_client_address = scheduler_host
                    if ":" in scheduler_host:
                        scheduler_host_has_port = True
                    if not scheduler_host_has_port:
                        if scheduler_port:
                            self._dask_client_address += ":" + scheduler_port
                        else:
                            self._dask_client_address += ":8786"
                    self._dask_client = DaskClient(self._dask_client_address)
            except Exception as err:
                if prev_dask_client:
                    self._dask_client = prev_dask_client
                if self._scheduler == "dask" and prev_scheduler == "spark":
                    self._scheduler = prev_scheduler
                raise MsPASSError(
                    "Runntime error: cannot create a dask client (Gateway="
                    + str(use_gw)
                    + "): "
                    + str(err),
                    "Fatal",
                )

            # Re-register the MongoDB worker plugin on the new client.
            try:
                mongo_plugin = MongoDBWorker(self, dbclient_key="dbclient")
                self._dask_client.register_plugin(
                    mongo_plugin, name="mongodb_worker"
                )
            except Exception:
                pass

            if hasattr(self, "_spark_context"):
                del self._spark_context
