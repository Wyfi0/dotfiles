# #### BEGIN GPL LICENSE BLOCK #####
#
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; either version 2
#  of the License, or (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software Foundation,
#  Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
#
# ##### END GPL LICENSE BLOCK #####

from concurrent.futures import (Future,
                                ThreadPoolExecutor,
                                TimeoutError)
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Callable, Dict, List, Optional, Sequence, Tuple
import functools
import os
import time
import webbrowser

from .assets import AssetType, SIZES, AssetData
from . import api
from . import asset_index
from . import env
from .logger import (DEBUG,
                     ERROR,
                     INFO,
                     initialize_logger,
                     NOT_SET,
                     WARNING)
from .notifications import NotificationSystem, Notification
from . import settings
from . import updater
from . import thread_manager as tm


DOWNLOAD_POLL_INTERVAL = 0.25
MAX_DOWNLOAD_RETRIES = 10
MAX_PARALLEL_ASSET_DOWNLOADS = 2
MAX_PARALLEL_DOWNLOADS_PER_ASSET = 8
SIZE_DEFAULT_POOL = 10


class SubscriptionState(Enum):
    """Values for allowed user subscription states."""
    NOT_POPULATED = 0
    FREE = 1,
    ACTIVE = 2,
    PAUSED = 3,
    CANCELLED = 4


@dataclass
class PoliigonSubscription:
    """Container object for a subscription."""

    plan_name: Optional[str] = None
    plan_credit: Optional[int] = None
    next_credit_renewal_date: Optional[datetime] = None
    next_subscription_renewal_date: Optional[datetime] = None
    plan_paused_at: Optional[datetime] = None
    plan_paused_until: Optional[datetime] = None
    subscription_state: Optional[SubscriptionState] = SubscriptionState.NOT_POPULATED
    period_unit: Optional[str] = None  # e.g. per "month" or "year" for renewing
    plan_price_id: Optional[str] = None
    plan_price: Optional[int] = None
    currency_code: Optional[str] = None  # e.g. "USD"
    base_price: Optional[int] = None
    currency_symbol: Optional[str] = None  # e.g. "$" (special character)


@dataclass
class PoliigonUser:
    """Container object for a user."""

    user_name: str
    user_id: int
    credits: Optional[int] = None
    credits_od: Optional[int] = None
    plan: Optional[PoliigonSubscription] = None


class PoliigonAddon():
    """Poliigon addon used for creating base singleton in DCC applications."""

    addon_name: str  # e.g. poliigon-addon-blender
    addon_version: tuple  # Current addon version
    software_source: str  # e.g. blender
    software_version: tuple  # DCC software version, e.g. (3, 0)

    library_paths: Sequence = []

    download_queue: Dict = {}
    purchase_queue: Dict = {}

    def __init__(self,
                 addon_name: str,
                 addon_version: tuple,
                 software_source: str,
                 software_version: tuple,
                 addon_env: env.PoliigonEnvironment,
                 addon_settings: settings.PoliigonSettings,
                 # See ThreadManager.__init__ for signature below,
                 #   e.g. print_exc(fut: Future, key_pool: PoolKeys)
                 callback_print_exc: Optional[Callable] = None):
        self.logger = initialize_logger(env=addon_env)
        self.logger_dl = initialize_logger("DL", env=addon_env)

        self.addon_name = addon_name
        self.addon_version = addon_version
        self.software_source = software_source
        self.software_version = software_version

        self.user = None
        self.login_error = None

        self.purchase_queue = {}
        self.download_cancelled = set()
        self.download_queue = {}

        self._env = addon_env

        self.set_logger_verbose(verbose=False)

        self._settings = addon_settings
        self._api = api.PoliigonConnector(
            env=self._env,
            software=software_source
        )
        self.logger.debug(f"API URL V1: {self._api.api_url}")
        self.logger.debug(f"API URL V2: {self._api.api_url_v2}")
        if "v1" in self._api.api_url and "apiv1" not in self._api.api_url:
            self.logger.warning("Likely you are running with an outdated API V1 URL")
        self._api.register_update(
            ".".join([str(x) for x in addon_version]),
            ".".join([str(x) for x in software_version])
        )
        self._tm = tm.ThreadManager(callback_print_exc=callback_print_exc)
        self.notify = NotificationSystem(self)
        self._updater = updater.SoftwareUpdater(
            addon_name=addon_name,
            addon_version=addon_version,
            software_version=software_version,
            notification_system=self.notify,
            local_json=self._env.local_updater_json
        )

        self.settings_config = self._settings.config

        base_dir = os.path.join(
            os.path.expanduser("~"),
            "Poliigon"
        )

        default_lib_path = os.path.join(base_dir, "Library")
        self.library_paths.append(default_lib_path)

        default_asset_index_path = os.path.join(
            base_dir,
            "AssetIndex",
            "asset_index.json",
        )
        self._asset_index = asset_index.AssetIndex(
            path_cache=default_asset_index_path, log=self.logger)

        self.online_previews_path = os.path.join(base_dir, "OnlinePreviews")
        try:
            os.makedirs(self.online_previews_path, exist_ok=True)
        except Exception:
            self.logger.exception(
                f"Failed to create directory: {self.online_previews_path}")

    # Decorator copied from comment in thread_manager.py
    def run_threaded(key_pool: tm.PoolKeys,
                     max_threads: Optional[int] = None,
                     foreground: bool = False) -> Callable:
        """Schedule a function to run in a thread of a chosen pool"""
        def wrapped_func(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapped_func_call(self, *args, **kwargs):
                args = (self, ) + args
                return self._tm.queue_thread(func, key_pool,
                                             max_threads, foreground,
                                             *args, **kwargs)
            return wrapped_func_call
        return wrapped_func

    def set_logger_verbose(self, verbose: bool) -> None:
        """To be used by DCC side to set main logger verbosity."""

        log_lvl_from_env = NOT_SET
        if self._env.config is not None:
            log_lvl_from_env = self._env.config.getint(
                "DEFAULT", "log_lvl", fallback=NOT_SET)
        if log_lvl_from_env != NOT_SET:
            self.logger.info(f"Log level forced by env: {log_lvl_from_env}")
            return
        log_lvl = INFO if verbose else ERROR
        self.logger.setLevel(log_lvl)

    def is_logged_in(self) -> bool:
        """Returns whether or not the user is currently logged in."""
        return self._api.token is not None and not self._api.invalidated

    def is_user_invalidated(self) -> bool:
        """Returns whether or not the user token was invalidated."""
        return self._api.invalidated

    def clear_user_invalidated(self):
        """Clears any invalidation flag for a user."""
        self._api.invalidated = False

    @run_threaded(tm.PoolKeys.INTERACTIVE)
    def log_in_with_credentials(self, email: str, password: str):
        self.clear_user_invalidated()

        req = self._api.log_in(
            email,
            password
        )

        if req.ok:
            user_data = req.body.get("user", {})

            self.create_user(user_data.get("name"), user_data.get("id"))

            self.login_error = None
        else:
            self.login_error = req.error

        return req

    def log_in_with_website(self):
        pass

    def check_for_survey_notice(
            self,
            free_user_url: str,
            plan_user_url: str,
            interval: int,
            label: str,
            tooltip: str = "",
            auto_enqueue: bool = True) -> None:

        already_shown = self.settings_config.get(
            "user", "survey_notice_shown", fallback=None)

        if already_shown not in [None, ""]:
            # Never notify again if already did once
            return

        first_local_asset = self.settings_config.get(
            "user", "first_local_asset", fallback=None)

        if first_local_asset in ["", None]:
            return

        def set_user_survey_flag() -> None:
            self.settings_config.set(
                "user", "survey_notice_shown", str(datetime.now()))
            self._settings.save_settings()

        first_asset_dl = datetime.strptime(first_local_asset, "%Y-%m-%d %H:%M:%S.%f")
        difference = datetime.now() - first_asset_dl
        if difference.days >= interval:
            self.notify.create_survey(
                is_free_user=self.is_free_user(),
                tooltip=tooltip,
                free_survey_url=free_user_url,
                active_survey_url=plan_user_url,
                label=label,
                auto_enqueue=auto_enqueue,
                on_dismiss_callable=set_user_survey_flag
            )


    @run_threaded(tm.PoolKeys.INTERACTIVE)
    def log_out(self):
        req = self._api.log_out()
        if req.ok:
            print("Logout success")
        else:
            print(req.error)

        self._api.token = None

        # Clear out user on logout.
        self.user = None

    def add_library_path(self, path: str, primary: bool = True):
        if not os.path.isdir(path):
            print("Path is not a directory!")
            return
        elif path in self.library_paths:
            print("Path already exists!")
            return

        if self.library_paths and primary:
            self.library_paths[0] = path
        else:
            self.library_paths.append(path)

    def get_library_path(self, primary: bool = True):
        if self.library_paths and primary:
            return self.library_paths[0]
        elif len(self.library_paths) > 1:
            # TODO(Mitchell): Return the most relevant lib path based on some input (?)
            return None
        else:
            return None

    def _get_user_info(self) -> Tuple:
        req = self._api.get_user_info()
        user_name = None
        user_id = None

        if req.ok:
            data = req.body
            user_name = data["user"]["name"]
            user_id = data["user"]["id"]
            self.login_error = None
        else:
            # TODO(SOFT-1029): Create an error log for fail in get user info
            self.login_error = req.error

        return user_name, user_id

    def _get_credits(self):
        req = self._api.get_user_balance()

        if req.ok:
            data = req.body
            self.user.credits = data.get("subscription_balance")
            self.user.credits_od = data.get("ondemand_balance")
        else:
            self.user.credits = None
            self.user.credits_od = None
            print(req.error)

    def _get_subscription_details(self):
        """Fetches the current user's subscription status."""
        req = self._api.get_subscription_details()

        if req.ok:
            plan = req.body
            if plan.get("plan_name") and plan["plan_name"] != api.STR_NO_PLAN:
                # TODO(SOFT-1030): Create User thread lock
                self.user.plan.plan_name = plan["plan_name"]
                self.user.plan.plan_credit = plan.get("plan_credit", None)

                # Extract "2022-08-19" from "2022-08-19 23:58:37"
                renew = plan.get("next_subscription_renewal_date", None)
                try:
                    renew = datetime.strptime(renew, "%Y-%m-%d %H:%M:%S")
                    self.user.plan.next_subscription_renewal_date = renew
                except (ValueError, TypeError):
                    self.user.plan.next_subscription_renewal_date = None

                next_credits = plan.get("next_credit_renewal_date", None)
                try:
                    next_credits = datetime.strptime(
                        next_credits, "%Y-%m-%d %H:%M:%S")
                    self.user.plan.next_credit_renewal_date = next_credits
                except (ValueError, TypeError):
                    self.user.plan.next_credit_renewal_date = None

                paused_plan_info = plan.get("paused_info", None)
                if paused_plan_info is not None:
                    self.user.plan.subscription_state = SubscriptionState.PAUSED
                    paused_date = paused_plan_info.get("pause_date", None)
                    resume_date = paused_plan_info.get("resume_date", None)

                    try:
                        self.user.plan.plan_paused_at = datetime.strptime(
                            paused_date, "%Y-%m-%d %H:%M:%S")
                        self.user.plan.plan_paused_until = datetime.strptime(
                            resume_date, "%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        self.user.plan.plan_paused_until = None
                        self.user.plan.plan_paused_at = None
                else:
                    self.user.plan.subscription_state = SubscriptionState.ACTIVE

                self.user.plan.period_unit = plan.get("period_unit", None)
                self.user.plan.plan_price_id = plan.get("plan_price_id", None)
                plan_price = plan.get("plan_price", None)
                try:
                    plan_price = int(plan_price)
                except ValueError:
                    plan_price = None
                self.user.plan.plan_price = plan_price
                self.user.plan.currency_code = plan.get("currency_code", None)
                try:
                    base_price = plan.get("base_price", None)
                except ValueError:
                    base_price = None
                self.user.plan.base_price = base_price
                currency_symbol = plan.get("currency_symbol", None)
                if currency_symbol is not None:
                    currency_symbol = currency_symbol[2:-1]  # e.g. &#36; => 36
                    currency_symbol = chr(int(currency_symbol))
                self.user.plan.currency_symbol = currency_symbol

            else:
                self.user.plan.plan_name = None
                self.user.plan.plan_credit = None
                self.user.plan.next_subscription_renewal_date = None
                self.user.plan.next_credit_renewal_date = None
                self.user.plan.subscription_state = SubscriptionState.FREE
                self.user.plan.period_unit = None
                self.user.plan.plan_price_id = None
                self.user.plan.plan_price = None
                self.user.plan.currency_code = None
                self.user.plan.base_price = None
                self.user.plan.currency_symbol = None

    @run_threaded(tm.PoolKeys.INTERACTIVE)
    def update_plan_data(self, done_callback: Optional[Callable] = None) -> None:
        # TODO(Joao): sub thread the two private functions
        self._get_credits()
        self._get_subscription_details()
        if done_callback is not None:
            done_callback()

    def create_user(
            self,
            user_name: Optional[str] = None,
            user_id: Optional[int] = None,
            done_callback: Optional[Callable] = None) -> Optional[Future]:

        if user_name is None or user_id is None:
            user_name, user_id = self._get_user_info()

        if user_name is None or user_id is None:
            return None

        self.user = PoliigonUser(
            user_name=user_name,
            user_id=user_id,
            plan=PoliigonSubscription(
                subscription_state=SubscriptionState.NOT_POPULATED)
        )

        future = self.update_plan_data(done_callback)
        return future

    def is_free_user(self):
        """Identifies a free user which neither
        has a plan nor on demand credits."""

        sub_state = self.user.plan.subscription_state
        free_plan = sub_state == SubscriptionState.FREE
        no_credits = self.user.credits in [0, None]
        no_od_credits = self.user.credits_od in [0, None]

        return free_plan and no_credits and no_od_credits

    def is_purchase_queued(self, asset_id):
        """Checks if an asset is queued for purchase"""
        queued = asset_id in self.purchase_queue.keys()
        return queued

    # TODO(part of SOFT-456): Enclose the following func in a thread lock.
    def queue_purchase(
            self, asset_data, search, category, callback: Callable = None):
        """Enqueue purchase request and return the Future object"""
        print(f"Queued asset for purchase{asset_data.asset_id}")
        future = self.purchase_asset(asset_data, search, category, callback)
        self.purchase_queue[asset_data.asset_id] = future

        return future

    @run_threaded(tm.PoolKeys.INTERACTIVE)
    def purchase_asset(self, asset_data, search, category, callback: Callable = None):
        """Create a thread to purchase an asset"""
        req = self._api.purchase_asset(asset_data.asset_id, search, category)

        # TODO(part of SOFT-456): Enclose the following del line in a thread lock.
        del self.purchase_queue[asset_data.asset_id]

        if req.ok:
            print(f"Purchased asset {asset_data.asset_id}")
            self._asset_index.mark_purchased(asset_data.asset_id)

        else:
            print(f"Failed to purchase asset {asset_data.asset_id}", str(req.error), str(req.body))

        if callback is not None:
            callback()

        return req

    def get_thumbnail_path(self, asset_name, index):
        """Return the best fitting thumbnail preview for an asset.

        The primary grid UI preview will be named asset_preview1.png,
        all others will be named such as asset_preview1_1K.png
        """
        if index == 0:
            # 0 is the small grid preview version of _preview1.

            # Fallback to legacy option of .jpg files if .png not found.
            thumb = os.path.join(
                self.online_previews_path,
                asset_name + "_preview1.png"
            )
            if not os.path.exists(thumb):
                thumb = os.path.join(
                    self.online_previews_path,
                    asset_name + "_preview1.jpg"
                )
        else:
            thumb = os.path.join(
                self.online_previews_path,
                asset_name + f"_preview{index}_1K.png")
        return thumb

    def is_download_queued(self, asset_id):
        """Checks if an asset is queued for download"""
        cancelled = asset_id in self.download_cancelled
        queued = asset_id in self.download_queue.keys()
        return queued and not cancelled

    def should_continue_asset_download(self, asset_id):
        """Check for any user cancel presses."""
        return asset_id not in self.download_cancelled

    def update_asset_data(self,
                          asset_id,
                          download_dir,
                          primary_files,
                          add_files):
        dbg = 1
        self.print_debug("update_asset_data", dbg=dbg)
        if not os.path.exists(download_dir):
            self.print_debug("update_asset_data NO DIR", dbg=dbg)
            return
        asset_files = []
        for path, dirs, files in os.walk(download_dir):
            asset_files += [os.path.join(path, file) for file in files if not file.endswith(api.DOWNLOAD_TEMP_SUFFIX)]
        if len(asset_files) == 0:
            self.print_debug("update_asset_data NO FILES", dbg=dbg)
            return
        # Ensure previously found asset files are added back
        asset_files += primary_files + add_files
        asset_files = list(set(asset_files))
        self._asset_index.update_from_directory(asset_id, download_dir)
        self.print_debug("update_asset_data DONE", dbg=dbg)

    def queue_download(
            self,
            asset_data: AssetData,
            size: str = None,
            done_callback: Callable = None,
            update_callback: Callable = None,
            native_mesh: bool = False,
            renderer: str = None) -> Future:
        """Enqueue download request and return the Future object

        Args:
           asset_data: Asset Data structure do be downloaded
           size: Textures Size to download
           done_callback: Callable to run in the same thread when download is done
           update_callback: Callable to update addon when the download process updates
           native_mesh: Boolean to define if just native file should be downloaded
           renderer: String to define which Renderer to download if native_mesh is true
        """

        print(f"Queued asset {asset_data.asset_id} for download!")

        self.download_queue[asset_data.asset_id] = {
            "data": asset_data,
            "size": size,
            "download_size": None,
            "download_percent": 0.0
        }

        future = self.download_asset(
            asset_data, size, done_callback, update_callback, native_mesh, renderer)
        self.download_queue[asset_data.asset_id]["future"] = future

        return future

    @run_threaded(tm.PoolKeys.ASSET_DL, MAX_PARALLEL_ASSET_DOWNLOADS)
    def download_asset(
            self,
            asset_data: AssetData,
            size: str,
            done_callback: Callable = None,
            update_callback: Callable = None,
            native_mesh: bool = False,
            renderer: str = None
    ) -> bool:
        """Create a thread to download an asset"""

        dbg = False
        asset_id = asset_data.asset_id
        asset_name = asset_data.asset_name

        # A queued download (user started more than MAX_PARALLEL_ASSET_DOWNLOADS)
        # may have been cancelled again before we reach this point
        user_cancel = asset_id in self.download_cancelled
        if user_cancel:
            # self.print_debug("download_asset_thread CANCEL BEFORE START", dbg=dbg)
            del self.download_queue[asset_id]
            self.download_cancelled.remove(asset_id)
            return False
        if asset_id not in self.download_queue:
            # self.print_debug("download_asset_thread DOWNLOAD NOT QUEUED", dbg=dbg)
            return False

        t_start = time.monotonic()

        asset_size = self.download_queue[asset_id]['size']
        asset_data = self.download_queue[asset_id]['data']

        download_data = self.get_download_data(
            asset_data, size=asset_size, native_mesh=native_mesh, renderer=renderer)

        self.print_debug("download_asset_thread", download_data, dbg=dbg)

        library_dir, primary_files, add_files = self.get_destination_library_directory(asset_data)
        download_dir = os.path.join(library_dir, asset_data.asset_name)

        try:
            os.makedirs(download_dir, exist_ok=True)
        except PermissionError:
            # TODO(SOFT-1200): Update with new download approach when ready.
            done_callback(asset_id, api.ERR_OS_NO_PERMISSION)
            del self.download_queue[asset_id]
            return False
        except OSError:
            done_callback(asset_id, api.ERR_OS_WRITE)
            del self.download_queue[asset_id]
            return False

        self.print_debug(f"download_asset_thread downloading to: {download_dir}")

        tpe = ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS_PER_ASSET)

        size_asset = 0
        retries = MAX_DOWNLOAD_RETRIES
        all_done = False
        user_cancel = False

        self.print_debug("download_asset_thread LOOP", dbg=dbg)
        while not all_done and not user_cancel and retries > 0:
            user_cancel = not self.download_update(asset_id, 1, 0.001)  # Init progress bar

            # Pass in retry so we can avoid signalling track event on retry.
            is_retry = retries < MAX_DOWNLOAD_RETRIES

            t_start_urls = time.monotonic()
            dl_list, size_asset = self.get_download_list(
                asset_id, download_data, is_retry)
            t_end_urls = time.monotonic()
            duration_urls = t_end_urls - t_start_urls

            user_cancel = not self.download_update(asset_id, 1, 0.001)
            if user_cancel:
                self.print_debug("download_asset_thread USER CANCEL", dbg=dbg)
                break
            elif dl_list is None:
                self.print_debug("download_asset_thread URL RETRIEVE -> no downloads", dbg=dbg)
                retries -= 1
                continue  # retry

            self.print_debug(f"=== Requesting URLs took {duration_urls:.3f} s.", dbg=dbg)

            self.schedule_downloads(tpe, dl_list, download_dir)

            self.print_debug("download_asset_thread POLL LOOP", dbg=dbg)
            while not all_done and not user_cancel:
                time.sleep(DOWNLOAD_POLL_INTERVAL)
                all_done, any_error, size_downloaded, res_error = self.check_downloads(dl_list)
                # Get user cancel and update progress UI
                percent_downloaded = max(size_downloaded / size_asset, 0.001)
                user_cancel = not self.download_update(asset_id,
                                                       size_asset,
                                                       percent_downloaded,
                                                       update_callback)
                if all_done and not any_error:
                    self.print_debug("download_asset_thread ALL DONE", dbg=dbg)
                    retries = 0
                    break
                elif any_error:
                    done_callback(asset_id, res_error)
                    self.cancel_downloads(dl_list)
                    return False
                elif user_cancel:
                    self.print_debug("download_asset_thread CANCELLING", dbg=dbg)
                    # TODO(Andreas): If cancelling due to expired link error,
                    #                maybe the DOWNLOADING ones should be
                    #                allowed to finish first
                    self.cancel_downloads(dl_list)
                    break
            retries -= 1

        # TODO(Andreas): what if retries exhausted?
        #                I'd probably opt for opening some error requester

        if all_done and not any_error and not user_cancel:
            self.rename_downloads(dl_list)

        self.update_asset_data(asset_id, download_dir,
                               primary_files, add_files)
        self.print_debug("download_asset_thread REMOVE FROM DL QUEUE", dbg=dbg)
        try:
            del self.download_queue[asset_id]
        except KeyError:
            pass  # Already removed.
        try:
            self.download_cancelled.remove(asset_id)
        except KeyError:
            pass  # Already removed or never existed.

        if all_done and not any_error and not user_cancel:
            self._download_asset_print_time(
                t_start, asset_name, dl_list, size_asset)

        if done_callback is not None:
            done_callback(asset_id)

        return True

    def download_update(
            self,
            asset_id: str,
            download_size: str,
            download_percent: float = 0.0,
            update_callback: Callable = None):

        """Updates info for download progress bar, return false to cancel."""

        if asset_id in self.download_queue.keys():
            self.download_queue[asset_id]["download_size"] = download_size
            self.download_queue[asset_id]["download_percent"] = download_percent

        if update_callback is not None:
            update_callback()

        return self.should_continue_asset_download(asset_id)

    def get_destination_library_directory(self, asset_data):
        # Usually the asset will be downloaded into the primary library.
        # Exception: There are already files for this asset located in another
        #            library (and only in this, _not_ in primary).
        dbg = 0
        self.print_debug("get_destination_library_directory", dbg=dbg)
        asset_name = asset_data.asset_name

        library_dir = self.get_library_path()
        primary_files = []
        add_files = []
        if not asset_data.is_local:
            return library_dir, primary_files, add_files

        for file in self._asset_index.get_files(asset_data.asset_id).keys():
            if not os.path.exists(file):
                continue
            if file.split(asset_name, 1)[0] == library_dir:
                primary_files.append(file)
            else:
                add_files.append(file)

        self.print_debug(dbg, "get_destination_library_directory",
                         "Found asset files in primary library:",
                         primary_files)

        if len(primary_files) == 0 and len(add_files) > 0:
            # Asset must be located in an additional directory
            #
            # Always download new maps to the highest-level directory
            # containing asset name, regardless of any existing (sub)
            # structure within that directory
            file = add_files[0]
            if asset_name in os.path.dirname(file):
                library_dir = file.split(asset_name, 1)[0]
                self.print_debug(dbg,
                                 "get_destination_library_directory",
                                 library_dir)

        self.print_debug("get_destination_library_directory DONE", dbg=dbg)
        return library_dir, primary_files, add_files

    def get_download_data(self,
                          asset_data: Dict,
                          size: Optional[str] = None,
                          size_bg: Optional[str] = None,
                          download_lods: Optional[bool] = None,
                          native_mesh: Optional[bool] = False,
                          renderer: Optional[str] = None):
        """Construct the data needed for the download.

        Note: In case a download parameter (e.g. size) is not specified,
              this function uses defaults from settings_config.
              If DCC side does not use settings_config, download parameters
              need to be properly provided under all circumstances (i.E. pass
              in DCC's default instead of missing parameter).

        Args:
            asset_data: Original asset data structure.
            size: Intended download size like '4K', fallback to pref default.
            size_bg: Alternate size for HDRI backgroud
            download_lods: Additioally download LOD FBXs
            native_mesh: boolean to define if downloads only native files
            renderer: string to define which renderer download the native model
        """

        # TODO(Andreas): LACKS SUPPORT FOR HDRI LIGHT

        incl_watermarked = size == "WM"

        type_data = asset_data.get_type_data()
        sizes_data = type_data.get_size_list(incl_watermarked)
        workflow = type_data.get_workflow()

        # TODO(Andreas): Here the logger causes issues, as it is not able to
        #                connvert some of our data structures as print seemed to be
        # self.logger_dl.debug("get_download_data", "asset_data", asset_data)
        # self.logger_dl.debug("get_download_data", "type_data", type_data)
        # self.logger_dl.debug("get_download_data", "sizes_data", sizes_data)
        # self.logger_dl.debug("get_download_data", "workflow", workflow)

        download_data = {
            "assets": [
                {
                    "id": asset_data.asset_id,
                    "name": asset_data.asset_name
                }
            ]
        }

        # If no size specified use defaults from preferences
        if size in ["", None]:
            if asset_data.asset_type == AssetType.TEXTURE:
                sizes = [self.settings_config.get("download", "tex_res")]
            elif asset_data.asset_type == AssetType.MODEL:
                settings_size = self.settings_config.get("download", "model_res")
                size_default = asset_data.model.size_default
                if settings_size in ["", "NONE", None] and size_default is not None:
                    sizes = [size_default]
                else:
                    sizes = [settings_size]
            elif asset_data.asset_type == AssetType.HDRI:
                sizes = [self.settings_config.get("download", "hdri_bg")]
                # TODO(Andreas): Really bg and what about light?
            elif asset_data.asset_type == AssetType.BRUSH:
                sizes = [self.settings_config.get("download", "brush")]
        else:
            sizes = [size]

        if asset_data.asset_type in [AssetType.HDRI, AssetType.TEXTURE]:
            map_codes = type_data.get_map_type_code_list(workflow)

            download_data["assets"][0]["workflows"] = [workflow]
            download_data["assets"][0]["type_codes"] = map_codes

        elif asset_data.asset_type == AssetType.MODEL:
            if download_lods is None:
                download_lods = self.settings_config.getboolean(
                    "download", "download_lods")
            download_data["assets"][0]["lods"] = int(download_lods)

            if native_mesh and renderer is not None:
                download_data["assets"][0]["softwares"] = [self._api.software_dl_dcc]
                download_data["assets"][0]["renders"] = [renderer]
            else:
                download_data["assets"][0]["softwares"] = ["ALL_OTHERS"]

        elif asset_data.asset_type == AssetType.BRUSH:
            # No special data needed for Brushes
            pass

        download_data["assets"][0]["sizes"] = [
            _size for _size in sizes if _size in sizes_data]

        # If no valid size found, try to find at least one matching asset's
        # available size data
        if len(download_data["assets"][0]["sizes"]) == 0:
            # TODO(Andreas): Why reversed (WM -> HIRES -> 18K -> ... -> 1K)?
            for _size in reversed(SIZES):
                if _size in sizes_data:
                    download_data["assets"][0]["sizes"] = [_size]
                    break

        # If still no valid size found, request minimum size
        if len(download_data["assets"][0]["sizes"]) == 0:
            msg = "Missing sizes for download! Setting size to minimum value."
            download_data["assets"][0]["sizes"] = [SIZES[0]]
            self.logger_dl.warning(msg)

        return download_data

    def get_download_list(self,
                          asset_id: int,
                          download_data: Dict,
                          is_retry: bool = False
                          ) -> Tuple[Optional[List[api.FileDownload]], int]:
        """Requests download URLs for an asset.

        Returns Tuple:
        Tuple[0]: Optional download list
        Tuple[1]: Size of all files in download list in bytes.
        """

        dl_list = None
        size_asset = 0
        res = self._api.download_asset_get_urls(
            asset_id, download_data, is_retry)
        if res.ok:
            dl_list = res.body.get("downloads", None)
            size_asset = res.body.get("size_asset", 0)
            if dl_list is None:
                self.logger_dl.error("Download list is None despite success")
            elif len(dl_list) == 0:
                self.logger_dl.error("Empty download list despite success")
        else:
            # Error is handled outside, including retries
            self.logger_dl.error(f"URL retrieve error\n{res}")

        self.logger_dl.debug("Done")
        return dl_list, size_asset

    def schedule_downloads(self,
                           tpe: ThreadPoolExecutor,
                           dl_list: List[api.FileDownload],
                           directory: str
                           ) -> None:
        """Submits downloads to thread pool."""

        self.logger_dl.debug("Schedule...")

        dl_list.sort(key=lambda dl: dl.size_expected)

        for download in dl_list:
            download.directory = directory
            # Andreas: Could also check here, if already DONE and not start
            #          the thread at all.
            #          Yet, I decided to prefer it handled by the thread itself.
            #          In this way the flow is always identical.
            download.status = api.DownloadStatus.WAITING
            download.fut = tpe.submit(self._api.download_asset_file,
                                      download=download)
            self.logger_dl.debug(f"Submitted {download.filename}")
        self.logger_dl.debug("Done")

    def check_downloads(
            self, dl_list: List[api.FileDownload]) -> Tuple[bool, bool, int, str]:
        """Returns download status flags and the number of downloaded bytes.

        Returns Tuple:
            Tuple[0]: all_done
            Tuple[1]: any_error
            Tuple[2]: size_downloaded
            Tuple[3]: res_error
        """

        any_error = False
        res_error = None
        all_done = True
        size_downloaded = 0

        self.logger_dl.debug(dl_list)

        for download in dl_list:
            size_downloaded += download.size_downloaded

            fut = download.fut
            if not fut.done():
                all_done = False
                continue

            res = fut.result()
            exc = fut.exception()
            had_excp = exc is not None
            if not res.ok or had_excp:
                if had_excp:
                    self.logger_dl.error(exc)
                any_error = True
                all_done = False
                res_error = res.error
                break
        return all_done, any_error, size_downloaded, res_error

    def cancel_downloads(self, dl_list: List[api.FileDownload]) -> None:
        """Cancels all download threads"""

        self.logger_dl.debug("Start cancel")

        for download in dl_list:
            download.set_status_cancelled()
            download.fut.cancel()

        # Wait for threads to actually return
        self.logger_dl.debug("Waiting")
        for download in dl_list:
            if download.fut.cancelled():
                continue
            try:
                download.fut.result(timeout=60)
            except TimeoutError:
                # TODO(Andreas): Now there seems to be some real issue...
                raise
            except BaseException:
                # The following line only works in Python 3.8+
                # self.print_debug(f"Unexpected {err=}, {type(err)=}", dbg=dbg)
                self.logger_dl.exception("Unexpected")
                raise

        self.logger_dl.debug("Done")

    def rename_downloads(
            self, dl_list: List[api.FileDownload]) -> Tuple[bool, str]:
        """Renames dowhloaded temp file."""

        self.logger_dl.debug("Start rename")

        error_msg = ""
        all_successful = True
        for download in dl_list:
            if download.status != api.DownloadStatus.DONE:
                self.logger_dl.warning(("File status not done despite "
                                        "all files reported done!"))
            path_temp = download.get_path(temp=True)
            temp_exists = os.path.exists(path_temp)
            path_final = download.get_path(temp=False)
            final_exists = os.path.exists(path_final)
            if not temp_exists and final_exists:
                continue

            try:
                os.rename(path_temp, path_final)
            except FileExistsError:
                os.remove(path_temp)
            except FileNotFoundError:
                download.status = api.DownloadStatus.ERROR
                download.error = f"Missing file: {path_temp}"
                self.logger_dl.error(
                    ("Neither temp download file nor target do exist\n"
                     f"    {path_temp}\n"
                     f"    {path_final}"))
                all_successful = False
            except PermissionError:
                # Note from Andreas:
                # I am not entirely sure, how this can happen (after all we
                # just downloaded the file...).
                # My assumption is, that somehow the download thread (while
                # already being done) did not actually exit, yet, maybe due to
                # some scheduling mishaps and is still keeping a handle to the
                # file. If I am correct, maybe a "sleep(0.1 sec)" and another
                # attempt to rename could get us out of this.
                # But that's of course pretty ugly and we should discuss
                # first, if we want to try something like this or just let
                # the download fail.
                download.status = api.DownloadStatus.ERROR
                download.error = ("Lacking permission to rename downloaded"
                                  f" file: {path_temp}")
                self.logger_dl.error(
                    (f"No permission to rename download:\n  from: {path_temp}"
                     f"\n  to: {path_final}"))
                all_successful = False

            # Gets the first error found to give feedback for the user
            if error_msg is not None and download.error not in [None, ""]:
                error_msg = download.error

        self.logger_dl.debug(f"Done, succeess = {all_successful}")
        return all_successful, error_msg

    def _download_asset_print_time(self,
                                   t_start: float,
                                   asset_name: str,
                                   dl_list: List[api.FileDownload],
                                   size_asset_bytes: int
                                   ) -> None:
        """Used in download_asset[_sync]() to print download timing results."""

        t_end = time.monotonic()
        duration = t_end - t_start
        size_MB = size_asset_bytes / (1024 * 1024)
        speed = size_MB / duration
        self.logger_dl.info(f"=== Successfully downloaded {asset_name}")
        self.logger_dl.info((f"    Entire Asset : {size_MB:.2f} MB, "
                             f"{duration:.3f} s, {speed:.2f} MB/s"))
        for download in dl_list:
            size_MB = download.size_downloaded / (1024 * 1024)
            speed = size_MB / download.duration
            self.logger_dl.info((f"    {download.filename} : {size_MB:.2f} MB,"
                                 f" {download.duration:.3f} s,"
                                 f" {speed:.2f} MB/s"))

    def _download_asset_loop_poll(self,
                                  asset_data: AssetData,
                                  dl_list: List[api.FileDownload],
                                  all_done: bool,
                                  update_callback: Callable
                                  ) -> bool:
        """Used in download_asset_sync to poll results inside download loop."""

        self.logger_dl.debug("Poll Loop")
        while not all_done:
            time.sleep(DOWNLOAD_POLL_INTERVAL)

            all_done, any_error, size_downloaded_bytes, res_error = self.check_downloads(
                dl_list)
            if any_error:
                asset_data.state.dl.set_error(error_msg=res_error)

            # Get user cancel and update progress UI
            size_asset_bytes = asset_data.state.dl.get_downloaded_bytes()
            progress = size_downloaded_bytes / size_asset_bytes
            asset_data.state.dl.set_progress(max(progress, 0.001))
            try:
                update_callback()
            except TypeError:
                pass  # no update callback

            any_error = asset_data.state.dl.has_error()
            if all_done and not any_error:
                self.logger_dl.debug("All Done :)")
                break
            is_cancelled = asset_data.state.dl.is_cancelled()
            if any_error or is_cancelled:

                self.logger_dl.debug((f"Cancelling... {any_error},"
                                      f" {is_cancelled}"))
                # TODO(Andreas): If cancelling due to expired link error,
                #                maybe the DOWNLOADING ones should be
                #                allowed to finish first
                self.cancel_downloads(dl_list)
                break

        return all_done

    def _asset_download_loop(self,
                             asset_data: AssetData,
                             download_data: Dict,
                             update_callback: Callable
                             ) -> Tuple[bool, List[api.FileDownload]]:
        """The actual download loop in download_asset_sync().

        Returns Tuple:
            Tuple[0]: all_done
            Tuple[1]: dl_list
        """

        tpe = ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS_PER_ASSET)

        retries = MAX_DOWNLOAD_RETRIES
        all_done = False
        dl_list = None

        self.logger_dl.debug("Download Loop")

        while not all_done and retries > 0:
            asset_data.state.dl.set_progress(0.001)

            if asset_data.state.dl.is_cancelled():
                break

            try:  # Init progress bar
                update_callback()
            except TypeError:
                pass  # No update callback

            is_retry = retries < MAX_DOWNLOAD_RETRIES

            t_start_urls = time.monotonic()
            # Pass in retry so we can avoid signalling track event on retry.
            dl_list, size_asset_bytes = self.get_download_list(
                asset_data.asset_id, download_data, is_retry)
            asset_data.state.dl.set_downloaded_bytes(size_asset_bytes)

            t_end_urls = time.monotonic()
            duration_urls = t_end_urls - t_start_urls

            if asset_data.state.dl.is_cancelled():
                self.logger_dl.debug("User Cancel")
                break
            elif dl_list is None:
                msg = "URL retrieve: No downloads"
                self.logger_dl.warning(
                    f"{msg} -> Retry ({retries})")
                retries -= 1
                if retries == 0:
                    asset_data.state.dl.set_error(error_msg=msg)
                continue  # retry

            self.logger_dl.info(f"=== Requesting URLs took {duration_urls:.3f} s.")

            self.schedule_downloads(
                tpe, dl_list, asset_data.state.dl.get_directory())

            all_done = self._download_asset_loop_poll(
                asset_data,
                dl_list,
                all_done,
                update_callback
            )
            retries -= 1

        return all_done, dl_list

    def set_first_local_asset(self, force_update: bool = False) -> None:
        """Conditionally assigns the current date to the settings file.

        Meant to be used in conjunction with surveying, this should be called
        either on first download or first import, if the value hasn't already
        been set or if force_update is true."""

        first_asset_timestamp = self.settings_config.get(
            "user", "first_local_asset", fallback="")
        if first_asset_timestamp == "" or force_update:
            time_stamp = datetime.now()
            self.settings_config.set(
                "user", "first_local_asset", str(time_stamp))
            self._settings.save_settings()

    def download_asset_sync(self,
                            asset_data: AssetData,
                            size: str,
                            download_lods: bool,
                            native_mesh: bool = False,
                            renderer: Optional[str] = None,
                            update_callback: Optional[Callable] = None,
                            dir_target: Optional[str] = None
                            ) -> bool:
        """Synchronously download an asset.

        Note: Also take notice of note on get_download_data().
        Note: Compared to download_asset, there is no download or cancel queue,
              in here. The download state is kept directly in asset_data.
              Thus there is no cancel callback, either. Instead simply set the
              cancel flag in asset_data and the download will cancel.
              Similarly upon UI redraw the progress can simply be read from
              asset_data, anytime.
        TODO(Andreas): Equal is true for download errors, which will be provided
                       in asset_data, too.
        """

        self.logger_dl.debug("Start")
        asset_name = asset_data.asset_name

        asset_data.state.dl.start()

        # A queued download (more than MAX_PARALLEL_ASSET_DOWNLOADS active)
        # may have been cancelled again before we reach this point
        # if asset_data.download_cancelled:
        if asset_data.state.dl.is_cancelled():
            self.logger_dl.debug("Cancel before start")
            return False

        t_start = time.monotonic()

        download_data = self.get_download_data(asset_data,
                                               size,
                                               size_bg=None,
                                               download_lods=download_lods,
                                               native_mesh=native_mesh,
                                               renderer=renderer)
        library_dir, primary_files, add_files = self.get_destination_library_directory(
            asset_data)
        if dir_target is None:
            download_dir = os.path.join(library_dir, asset_name)
        else:
            download_dir = os.path.join(dir_target, asset_name)

        try:
            os.makedirs(download_dir, exist_ok=True)
        except PermissionError:
            # TODO(SOFT-1265): communicate issue
            asset_data.state.dl.set_error(error_msg=api.ERR_OS_NO_PERMISSION)
            self.logger_dl.exception(f"{api.ERR_OS_NO_PERMISSION}: {download_dir}")
            return False
        except OSError as e:
            asset_data.state.dl.set_error(error_msg=str(e))
            self.logger_dl.exception(f"Download directory: {download_dir}")
            return False

        self.logger_dl.debug(f"Download directory: {download_dir}")
        asset_data.state.dl.set_directory(download_dir)

        all_done, dl_list = self._asset_download_loop(
            asset_data, download_data, update_callback)

        has_error = asset_data.state.dl.has_error()
        is_cancelled = asset_data.state.dl.is_cancelled()
        if not all_done or has_error or is_cancelled:
            return False

        result, error_msg = self.rename_downloads(dl_list)
        if not result:
            asset_data.state.dl.set_error(error_msg=error_msg)
            return False

        size_asset_bytes = asset_data.state.dl.get_downloaded_bytes()
        self._download_asset_print_time(
            t_start, asset_name, dl_list, size_asset_bytes)

        self.set_first_local_asset(force_update=False)

        return True

    @run_threaded(tm.PoolKeys.ASSET_DL, MAX_PARALLEL_ASSET_DOWNLOADS)
    def thread_download_asset(self,
                              asset_data: AssetData,
                              size: str,
                              download_lods: bool,
                              native_mesh: bool = False,
                              renderer: str = None,
                              done_callback: Callable = None,
                              update_callback: Callable = None
                              ) -> bool:
        """Calls download_asset_sync() in a thread to download an asset."""

        result = self.download_asset_sync(
            asset_data,
            size,
            download_lods,
            native_mesh,
            renderer,
            update_callback
        )
        try:
            done_callback()
        except TypeError:
            pass  # No done callback

        return result

    def print_debug(self, *args, dbg=False, bg=True):
        """Print out a debug statement with no separator line.

        Cache based on args up to a limit, to avoid excessive repeat prints.
        All args must be flat values, such as already casted to strings, else
        an error will be thrown.
        """
        if dbg:
            # Ensure all inputs are hashable, otherwise lru_cache fails.
            stringified = [str(arg) for arg in args]
            self._cached_print(*stringified, bg=bg)

    @lru_cache(maxsize=32)
    def _cached_print(self, *args, bg: bool):
        """A safe-to-cache function for printing."""
        print(*args)

    def open_asset_url(self, asset_id: int) -> None:
        asset_data = self._asset_index.get_asset(asset_id)
        url = self._api.add_utm_suffix(asset_data.url)
        webbrowser.open(url)

    def open_poliigon_link(self,
                           link_type: str,
                           add_utm_suffix: bool = True
                           ) -> None:
        """Opens a Poliigon URL"""

        # TODO(Andreas): As soon as P4B uses PoliigonAddon move codee from
        #                api.open_poliigon_link here and remove function in api
        self._api.open_poliigon_link(
            link_type, add_utm_suffix, env_name=self._env.env_name)

    def get_wm_download_path(self, asset_name: str) -> str:
        """Returns an asset name path inside the OnlinePreviews folder"""

        path_poliigon = os.path.dirname(self._settings.base)
        path_thumbs = os.path.join(path_poliigon, "OnlinePreviews")
        path_wm_previews = os.path.join(path_thumbs, asset_name)
        return path_wm_previews

    def download_material_wm(
            self, files_to_download: List[Tuple[str, str]]) -> None:
        """Synchronous function to download material preview."""

        urls = []
        files_dl = []
        for _url_wm, _filename_wm_dl in files_to_download:
            urls.append(_url_wm)
            files_dl.append(_filename_wm_dl)

        resp = self._api.pooled_preview_download(urls, files_dl)
        if not resp.ok:
            msg = f"Failed to download WM preview\n{resp}"
            self._api.report_message(
                "download_mat_preview_dl_failed", msg, "error")
            # Continue, as some may have worked.

        for _filename_wm_dl in files_dl:
            filename_wm = _filename_wm_dl[:-3]  # cut of _dl

            try:
                file_exists = os.path.exists(filename_wm)
                dl_exists = os.path.exists(_filename_wm_dl)
                if file_exists and dl_exists:
                    os.remove(filename_wm)
                elif not file_exists and not dl_exists:
                    raise FileNotFoundError
                if dl_exists:
                    os.rename(_filename_wm_dl, filename_wm)
            except FileNotFoundError:
                msg = f"Neither {filename_wm}, nor {_filename_wm_dl} exist"
                self._api.report_message(
                    "download_mat_existing_file", msg, "error")
            except FileExistsError:
                msg = f"File {filename_wm} already exists, failed to rename"
                self._api.report_message(
                    "download_mat_rename", msg, "error")
            except Exception as e:
                self.logger.exception("Unexpected exception while renaming WM preview")
                msg = f"Unexpected exception while renaming {_filename_wm_dl}\n{e}"
                self._api.report_message(
                    "download_wm_exception", msg, "error")
        return resp
