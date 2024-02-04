
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

"""This module contains the API Remote Control."""

from concurrent.futures import Future
from dataclasses import dataclass
from enum import IntEnum, unique
from functools import partial
import os
from queue import Queue
from threading import Lock, Thread
from typing import Callable, Dict, List, Optional

from poliigon_core.addon import PoliigonAddon
from poliigon_core.api import ApiResponse
from poliigon_core.api_remote_control_params import (
    AddonRemoteControlParams,
    ApiJobParams,
    ApiJobParamsDownloadAsset,
    ApiJobParamsDownloadThumb,
    ApiJobParamsDownloadWMPreview,
    ApiJobParamsGetCategories,
    ApiJobParamsGetUserData,
    ApiJobParamsGetAssets,
    ApiJobParamsLogin,
    ApiJobParamsPurchaseAsset,
    CmdLoginMode
)
from poliigon_core.assets import AssetData
from poliigon_core.logger import initialize_logger


@unique
class JobType(IntEnum):
    LOGIN = 0
    GET_USER_DATA = 1  # credits, subscription, user info
    GET_CATEGORIES = 2
    GET_ASSETS = 10
    DOWNLOAD_THUMB = 11
    PURCHASE_ASSET = 12
    DOWNLOAD_ASSET = 13
    DOWNLOAD_WM_PREVIEW = 14
    EXIT = 99999


@dataclass
class ApiJob():
    """Describes an ApiJob and gets passed through the queues,
    subsequentyly being processed in thread_schedule and thread_collect.
    """

    job_type: JobType
    params: Optional[ApiJobParams] = None
    callback_cancel: Optional[Callable] = None
    callback_progess: Optional[Callable] = None
    callback_done: Optional[Callable] = None
    result: Optional[ApiResponse] = None
    future: Optional[Future] = None

    def __eq__(self, other):
        # sorry
        return self.job_type == other.job_type and self.params == other.params


class ApiRemoteControl():

    def __init__(self, addon: PoliigonAddon):
        # Only members defined in addon_core.PoliigonAddon are allowed to be
        # used inside this module
        self._addon = addon
        self._addon_params = AddonRemoteControlParams()
        self._tm = addon._tm
        self._api = addon._api
        self._asset_index = addon._asset_index

        self.logger = initialize_logger("APIRC", env=addon._env)

        self.queue_jobs = Queue()
        self.schedule_running = False
        self.thd_schedule = Thread(target=self._thread_schedule)
        self.thd_schedule.setName("API RC Schedule")
        self.thd_schedule.start()

        self.queue_jobs_done = Queue()
        self.collect_running = False
        self.thd_collect = Thread(target=self._thread_collect)
        self.thd_collect.setName("API RC Collect")
        self.thd_collect.start()

        self.lock_jobs_in_flight = Lock()
        self.jobs_in_flight = {}  # {job_type: [futures]}

        self.init_stats()

    def init_stats(self) -> None:
        """Initializes job statistics counters."""

        self.cnt_added = {}
        self.cnt_queued = {}
        self.cnt_cancelled = {}
        self.cnt_exec = {}
        self.cnt_done = {}
        for job_type in JobType.__members__.values():
            self.cnt_added[job_type] = 0
            self.cnt_queued[job_type] = 0
            self.cnt_cancelled[job_type] = 0
            self.cnt_exec[job_type] = 0
            self.cnt_done[job_type] = 0

    def get_stats(self) -> Dict:
        """Returns job statistics counters as a dictionary."""

        stats = {}
        stats["Jobs added"] = self.cnt_added
        stats["Jobs queued"] = self.cnt_queued
        stats["Jobs cancelled"] = self.cnt_cancelled
        stats["Jobs exec"] = self.cnt_exec
        stats["Jobs done"] = self.cnt_done
        return stats

    def add_job_login(self,
                      mode: CmdLoginMode = CmdLoginMode.LOGIN_BROWSER,
                      email: Optional[str] = None,
                      pwd: Optional[str] = None,
                      time_since_enable: Optional[int] = None,
                      callback_cancel: Callable = None,
                      callback_progess: Callable = None,
                      callback_done: Callable = None,
                      force: bool = True
                      ) -> None:
        """Convenience function to add a login or logout job."""

        params = ApiJobParamsLogin(mode, email, pwd, time_since_enable)
        self.add_job(JobType.LOGIN,
                     params,
                     callback_cancel,
                     callback_progess,
                     callback_done,
                     force)

    def add_job_get_user_data(self,
                              user_name: str,
                              user_id: str,
                              callback_cancel: Callable = None,
                              callback_progess: Callable = None,
                              callback_done: Callable = None,
                              force: bool = True
                              ) -> None:
        """Convenience function to add a get user data job."""

        params = ApiJobParamsGetUserData(user_name, user_id)
        self.add_job(JobType.GET_USER_DATA,
                     params,
                     callback_cancel,
                     callback_progess,
                     callback_done,
                     force)

    def add_job_get_categories(self,
                               callback_cancel: Callable = None,
                               callback_progess: Callable = None,
                               callback_done: Callable = None,
                               force: bool = True
                               ) -> None:
        """Convenience function to add a get categories job."""

        params = ApiJobParamsGetCategories()
        self.add_job(JobType.GET_CATEGORIES,
                     params,
                     callback_cancel,
                     callback_progess,
                     callback_done,
                     force)

    def add_job_get_assets(self,
                           path_library: str,
                           tab: str,  # KEY_TAB_ONLINE, KEY_TAB_MY_ASSETS
                           category_list: List[str] = ["All Assets"],
                           search: str = "",
                           idx_page: int = 1,
                           page_size: int = 10,
                           force_request: bool = False,
                           do_get_all: bool = True,
                           callback_cancel: Callable = None,
                           callback_progess: Callable = None,
                           callback_done: Callable = None,
                           force: bool = True
                           ) -> None:
        """Convenience function to add a get assets job."""

        params = ApiJobParamsGetAssets(path_library,
                                       tab,
                                       category_list,
                                       search,
                                       idx_page,
                                       page_size,
                                       force_request,
                                       do_get_all)
        self.add_job(JobType.GET_ASSETS,
                     params,
                     callback_cancel,
                     callback_progess,
                     callback_done,
                     force)

    def add_job_download_thumb(self,
                               asset_id: int,
                               url: str,
                               path: str,
                               do_update: bool = False,
                               callback_cancel: Callable = None,
                               callback_progess: Callable = None,
                               callback_done: Callable = None,
                               force: bool = False
                               ) -> None:
        """Convenience function to add a download thumb job."""

        if not do_update and os.path.exists(path):
            return

        params = ApiJobParamsDownloadThumb(asset_id, url, path, do_update)
        self.add_job(JobType.DOWNLOAD_THUMB,
                     params,
                     callback_cancel,
                     callback_progess,
                     callback_done,
                     force)

    def add_job_purchase_asset(self,
                               asset_data: AssetData,
                               category_list: List[str] = ["All Assets"],
                               search: str = "",
                               job_download: Optional = None,  # type: ApiJob
                               callback_cancel: Callable = None,
                               callback_progess: Callable = None,
                               callback_done: Callable = None,
                               force: bool = True
                               ) -> None:
        """Convenience function to add a purchase asset job."""

        params = ApiJobParamsPurchaseAsset(asset_data,
                                           category_list,
                                           search,
                                           job_download)
        self.add_job(JobType.PURCHASE_ASSET,
                     params,
                     callback_cancel,
                     callback_progess,
                     callback_done,
                     force)

    def create_job_download_asset(self,
                                  asset_data: AssetData,
                                  size: str = "2K",
                                  size_bg: str = "",
                                  type_bg: str = "EXR",
                                  lod: str = "NONE",
                                  variant: str = "",
                                  download_lods: bool = False,
                                  native_mesh: bool = True,
                                  renderer: str = "",
                                  callback_cancel: Callable = None,
                                  callback_progess: Callable = None,
                                  callback_done: Callable = None
                                  ) -> None:
        """Convenience function to add a download asset job."""

        params = ApiJobParamsDownloadAsset(
            asset_data, size, size_bg, type_bg, lod, variant,
            download_lods, native_mesh, renderer)
        job = ApiJob(
            JobType.DOWNLOAD_ASSET, params, callback_cancel, callback_progess, callback_done)

        # Due to the limitation of the number of threads, the download thread
        # may not start immediately. In that case it would seem, as if nothing
        # is happening.
        asset_data.state.dl.start()
        if callback_progess is not None:
            callback_progess(job)

        return job

    def add_job_download_asset(self,
                               asset_data: AssetData,
                               size: str = "2K",
                               size_bg: str = "",
                               type_bg: str = "EXR",
                               lod: str = "NONE",
                               variant: str = "",
                               download_lods: bool = False,
                               native_mesh: bool = True,
                               renderer: str = "",
                               callback_cancel: Callable = None,
                               callback_progess: Callable = None,
                               callback_done: Callable = None,
                               force: bool = True
                               ) -> None:
        """Convenience function to add a download asset job."""

        self.cnt_added[JobType.DOWNLOAD_ASSET] += 1
        job = self.create_job_download_asset(
            asset_data,
            size,
            size_bg,
            type_bg,
            lod,
            variant,
            download_lods,
            native_mesh,
            renderer,
            callback_cancel,
            callback_progess,
            callback_done
        )
        self.enqueue_job(job, force)

    def add_job_download_wm_preview(self,
                                    asset_data: AssetData,
                                    renderer: str = "",
                                    callback_cancel: Callable = None,
                                    callback_progess: Callable = None,
                                    callback_done: Callable = None,
                                    force: bool = True
                                    ) -> None:
        """Convenience function to add a download WM preview job."""

        params = ApiJobParamsDownloadWMPreview(asset_data,
                                               renderer)
        self.add_job(JobType.DOWNLOAD_WM_PREVIEW,
                     params,
                     callback_cancel,
                     callback_progess,
                     callback_done,
                     force)

    def _is_job_already_enqueued(self, job: ApiJob) -> bool:
        """Returns True, if an identical job exists already."""

        with self.lock_jobs_in_flight:
            jobs_in_flight_copy = self.jobs_in_flight.copy()

        try:
            return job in jobs_in_flight_copy[job.job_type]
        except KeyError:
            return False

    def enqueue_job(self, job: ApiJob, force: bool = True) -> None:
        """Enqueúes a single ApiJob.

        Arguments:
        force: Default True, False: Enqueue only, if not queued already
        """

        if not force and self._is_job_already_enqueued(job):
            return

        self.cnt_queued[job.job_type] += 1
        self.queue_jobs.put(job)

    def add_job(self,
                job_type: JobType,
                params: Dict = {},
                callback_cancel: Callable = None,
                callback_progess: Callable = None,
                callback_done: Callable = None,
                force: bool = True,
                ) -> None:
        """Adds a job to be processed by API remote control."""

        self.cnt_added[job_type] += 1

        job = ApiJob(
            job_type, params, callback_cancel, callback_progess, callback_done)
        self.enqueue_job(job, force)

    def _release_job(self, job: ApiJob) -> None:
        """Removes a finished job from 'in flight' list."""

        try:
            with self.lock_jobs_in_flight:
                self.jobs_in_flight[job.job_type].remove(job)
        except (KeyError, ValueError):
            pass  # List of job type not found or job not found in list

    def shutdown(self) -> None:
        """Stops remote control's threads."""

        self.add_job(JobType.EXIT)
        self.wait_for_all()

    def _wait_for_type(self,
                       jobs_in_flight_copy: Dict,
                       job_type: JobType,
                       do_wait: bool,
                       timeout: int
                       ) -> None:
        """Cancels all jobs of given type, optionally waits until cancelled."""

        for job in jobs_in_flight_copy[job_type]:
            try:
                with self.lock_jobs_in_flight:
                    self.jobs_in_flight[job.job_type].remove(job)
            except (KeyError, AttributeError):
                pass

            if job.result is None:
                job.result = ApiResponse(ok=True,
                                         body={"data": []},
                                         error="job cancelled")

            if job.future is None:
                self.logger.warning(f"Future is None: {job.job_type.name}")
                continue
            elif job.future.cancel():
                self.cnt_cancelled[job.job_type] += 1
                continue
            try:
                job.callback_cancel()
            except TypeError:
                pass  # Not every job has a cancel callback
            if do_wait:
                job.future.result(timeout)

    def wait_for_all(self,
                     job_type: Optional[JobType] = None,
                     do_wait: bool = True,
                     timeout: Optional[int] = None
                     ) -> None:
        """Cancels all jobs or just a given type, optionally waits until
        cancelled.
        """

        with self.lock_jobs_in_flight:
            jobs_in_flight_copy = self.jobs_in_flight.copy()

        if job_type is None:
            for job_type in jobs_in_flight_copy:
                self._wait_for_type(
                    jobs_in_flight_copy, job_type, do_wait, timeout)
        elif job_type in jobs_in_flight_copy:
            self._wait_for_type(
                jobs_in_flight_copy, job_type, do_wait, timeout)

    def is_job_type_active(self, job_type: JobType) -> bool:
        """Returns True if there's at least one job of given type in flight."""

        return len(self.jobs_in_flight.get(job_type, [])) > 0

    def _thread_schedule(self) -> None:
        """Thread waiting on job queue to start jobs in thread pool."""

        self.schedule_running = True
        while self.schedule_running:
            job = self.queue_jobs.get()

            self.cnt_exec[job.job_type] += 1

            if job.job_type != JobType.EXIT:
                with self.lock_jobs_in_flight:
                    try:
                        self.jobs_in_flight[job.job_type].append(job)
                    except KeyError:
                        self.jobs_in_flight[job.job_type] = [job]

            if job.job_type != JobType.EXIT:
                job.future = self._tm.queue_thread(
                    job.params.thread_execute,
                    job.params.POOL_KEY,
                    max_threads=None,
                    foreground=False,
                    api_rc=self,
                    job=job
                )
            else:
                # JobType.EXIT
                self.queue_jobs_done.put(job)  # stop collector
                self.schedule_running = False

            def callback_enqueue_done(fut, job: ApiJob) -> None:
                self.queue_jobs_done.put(job)

            cb_done = partial(callback_enqueue_done, job=job)
            try:
                job.future.add_done_callback(cb_done)
            except AttributeError:  # JobType.EXIT has no Future
                if job.job_type != JobType.EXIT:
                    msg = f"Job {job.job_type.name} has no Future"
                    self.logger.exception(msg)

    def _thread_collect(self) -> None:
        """Thread awaiting threaded jobs to finish, then executes job's post
        processing.
        """

        self.collect_running = True
        while self.collect_running:
            job = self.queue_jobs_done.get()

            if job.job_type != JobType.EXIT:
                job.params.finish(self, job)
            else:
                # JobType.EXIT
                self.collect_running = False
                break

            try:
                job.callback_done(job)
            except TypeError:
                pass  # There is no done callback

            self._release_job(job)
            self.cnt_done[job.job_type] += 1
