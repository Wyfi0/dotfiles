
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

"""This module contains the API Remote Control parameter classes."""

from dataclasses import dataclass, field
from enum import IntEnum, unique
from functools import partial
import os
from typing import Callable, Dict, List, Optional, Tuple

from .addon import (PoliigonSubscription,
                    PoliigonUser,
                    SubscriptionState)
from .api import ApiResponse
from .assets import AssetData
from .thread_manager import PoolKeys


@unique
class CmdLoginMode(IntEnum):
    # Do not have zero value members!
    LOGIN_CREDENTIALS = 1
    LOGIN_BROWSER = 2
    LOGOUT = 3
    LOGIN_CANCEL = 4


# For use in query keys
KEY_TAB_IMPORTED = "imported"
KEY_TAB_MY_ASSETS = "my_assets"
KEY_TAB_ONLINE = "poliigon"

CATEGORY_ALL = "All Assets"

# CATEGORY_MAPPING_ dicts are used to translate category names between
# API convention (as received/sent from/to API) to local/plugin convention
# (as for example used in AssetIndex, see for example
# CATEGORY_NAME_TO_ASSET_TYPE in assets.py).
CATEGORY_MAPPING_API_TO_LOCAL = {
    # Key API convention: Local/plugin convention
    "HDRs": "HDRIs"
}
CATEGORY_MAPPING_LOCAL_TO_API = {
    # Key Local/plugin convention: API convention
    "HDRIs": "HDRs"
}


def get_search_key(tab: str, search: str, category_list: List[str]) -> str:
    """Returns a search/query string."""

    if search != "":
        sep = "@"
        search_ext = f"{sep}{search}"
    else:
        sep = "/"
        search_ext = ""
    categories = sep.join(category_list)
    key = f"{tab}{sep}{categories}{search_ext}"
    return key


# TODO(Andreas): Move these THUMB_SIZE_ constants to a better place.
THUMB_SIZE_MIN = 100
THUMB_SIZE_PROG = 145  # progress bar switches to short label
THUMB_SIZE_DEFAULT = 150
THUMB_SIZE_MAX = 200
THUMB_SIZE_DOWNLOAD = 300


@dataclass
class AddonRemoteControlParams():
    """ Parameters to be set in the Addon Side.

    NOTE: These processes are ran by default by API RC, so it is not possible
    to only parse using done_callback parameter in the ApiJob"""

    online_assets_chunk_size: int = 100
    my_assets_chunk_size: int = 100
    callback_get_categories_done: Optional[Callable] = None
    callback_get_asset_done: Optional[Callable] = None
    callback_get_user_data_done: Optional[Callable] = None


@dataclass
class ApiJobParams():
    """Job parameters.

    NOTE: Nothing in here.
          Only used as a parent class for type hints and defining interfaces.
    """

    def __eq__(self, other):
        raise NotImplementedError(
            "Some derived class forgot to implememnt __eq__")

    def thread_execute(self,
                       api_rc,  # : ApiRemoteControl
                       job  # : ApiJob
                       ) -> None:
        """Executes job in a thread, started from thread_schedule."""

        raise NotImplementedError

    def finish(self,
               api_rc,  # : ApiRemoteControl
               job  # : ApiJob
               ) -> None:
        """Finishes a job, called from thread_collect."""

        raise NotImplementedError


@dataclass
class ApiJobParamsLogin(ApiJobParams):
    """Login specific parameters"""

    mode: CmdLoginMode
    email: Optional[str] = None
    pwd: Optional[str] = None
    time_since_enable: Optional[int] = None

    POOL_KEY = PoolKeys.INTERACTIVE

    def __eq__(self, other):
        return self.mode == other.mode and self.email == other.email

    def _exec_job_login_credential(self, api_rc, job) -> None:
        """Executes a login with email and password.
        Gets called in thread (thread_exec_job_login).
        """

        job.result = api_rc._api.log_in(self.email,
                                        self.pwd,
                                        self.time_since_enable)

    def _exec_job_login_browser(self, api_rc, job) -> None:
        """Executes a login via browser.
        Gets called in thread (thread_exec_job_login).
        """

        job.result = api_rc._api.log_in_with_website(
            self.time_since_enable)
        if job.callback_cancel is not None and job.callback_cancel():
            return

        job.result = api_rc._api.poll_login_with_website_success(
            timeout=300,
            cancel_callback=job.callback_cancel,
            time_since_enable=self.time_since_enable
        )

    def _exec_job_logout(self, api_rc, job) -> None:
        """Executes a logout.
        Gets called in thread (thread_exec_job_login).
        """

        job.result = api_rc._api.log_out()

    def thread_execute(self, api_rc, job) -> None:
        """Executes any login/logout jobs in a thread,
        started from thread_schedule.
        """

        if self.mode == CmdLoginMode.LOGIN_CREDENTIALS:
            self._exec_job_login_credential(api_rc, job)
        elif self.mode == CmdLoginMode.LOGIN_BROWSER:
            self._exec_job_login_browser(api_rc, job)
        elif self.mode == CmdLoginMode.LOGOUT:
            self._exec_job_logout(api_rc, job)

    def finish(self, api_rc, job) -> None:
        """Finishes login/logout jobs, called from thread_collect."""

        # TODO(Andreas): Evaluate res and store in prefs?
        #                Currently done in CommandDataLogin.callback_xyz_done.
        #                Not sure, yet, where the better place would be.
        #                To have it here, would need abstract ways to set prefs.

        # TODO(Andreas): Strange! Should likely be fixed in addon-core.api
        #                or even server side. Will look into it at a later point.
        if "results" in job.result.body:
            # Login via browser response
            job.result.body = job.result.body["results"]

        if not job.result.ok or self.mode == CmdLoginMode.LOGOUT:
            api_rc.logger.debug("### Set user None")
            api_rc._addon.user = None
        else:
            user_info = job.result.body["user"]
            api_rc._addon.user = PoliigonUser(
                user_name=user_info["name"],
                user_id=user_info["id"],
                plan=PoliigonSubscription(
                    subscription_state=SubscriptionState.NOT_POPULATED))


@dataclass
class ApiJobParamsGetCategories(ApiJobParams):
    """Get Categories specific parameters"""

    # no parameters needed
    POOL_KEY = PoolKeys.INTERACTIVE

    def __eq__(self, other):
        return True

    def thread_execute(self, api_rc, job) -> None:
        """Executes any get categories jobs in a thread,
        started from thread_schedule.
        """

        job.result = api_rc._api.categories()

    def finish(self, api_rc, job) -> None:
        """Finishes get categories jobs, called from thread_collect."""

        # For the moment nothing to do here.
        # Will likely chanage with dynamic category counts.
        pass


@dataclass
class ApiJobParamsGetUserData(ApiJobParams):
    """Get User Data specific parameters"""

    user_name: str
    user_id: str

    POOL_KEY = PoolKeys.INTERACTIVE

    def __eq__(self, other):
        return self.user_name == other.user_name and self.user_id == other.user_id

    def _get_user_info_from_api(self, api_rc) -> bool:
        """Fetches the user information.
        Gets called in thread (thread_exec_job_get_user_data).
        """

        user_name, user_id = api_rc._addon._get_user_info()
        if api_rc._addon.login_error is None:
            api_rc._addon.user.user_name = user_name
            api_rc._addon.user.user_id = user_id
        else:
            api_rc.logger.error(api_rc._addon.login_error)

    def thread_execute(self, api_rc, job) -> None:
        """Executes any get user data jobs in a thread,
        started from thread_schedule.
        """

        if api_rc._addon.user is None:
            api_rc._addon.user = PoliigonUser(
                user_name=self.user_name,
                user_id=self.user_id,
                plan=PoliigonSubscription(
                    subscription_state=SubscriptionState.NOT_POPULATED))

        self._get_user_info_from_api(api_rc)
        if api_rc._addon.login_error is not None:
            api_rc.logger.error(
                f"get_user_info failed: {api_rc._addon.login_error}")
        else:
            try:
                api_rc._addon._get_credits()
                api_rc._addon._get_subscription_details()
            except Exception as e:
                # TODO(Andreas): Had an issue in the beginning, by now
                #                it should be fixed and exception no longer
                #                be an issue. Remove try/except
                api_rc.logger.error(f"### exc: _get_credits...\n{e}")

        # TODO(Andreas): maybe more checks needed?
        ok = api_rc._addon.user is not None
        ok &= api_rc._addon.login_error is None
        job.result = ApiResponse(ok=ok,
                                 body={},
                                 error="")

    def finish(self, api_rc, job) -> None:
        """Finishes get user data jobs, called from thread_collect."""

        if not job.result.ok:
            return

        # TODO(Andreas): Maybe have an option to select follow up jobs.
        #                See: https://github.com/poliigon/poliigon-addon-core/pull/188#discussion_r1346322792
        api_rc.add_job_get_categories(
            callback_cancel=None,
            callback_progess=None,
            callback_done=api_rc._addon_params.callback_get_categories_done,
            force=True

        )
        api_rc.add_job_get_assets(
            path_library=api_rc._addon.get_library_path(primary=True),
            tab=KEY_TAB_ONLINE,
            category_list=["All Assets"],
            search="",
            idx_page=1,
            page_size=api_rc._addon_params.online_assets_chunk_size,
            force_request=False,
            do_get_all=True,
            callback_cancel=None,
            callback_progess=None,
            callback_done=api_rc._addon_params.callback_get_asset_done,
            force=True
        )
        api_rc.add_job_get_assets(
            path_library=api_rc._addon.get_library_path(primary=True),
            tab=KEY_TAB_MY_ASSETS,
            category_list=["All Assets"],
            search="",
            idx_page=1,
            page_size=api_rc._addon_params.my_assets_chunk_size,
            force_request=True,
            do_get_all=True,
            callback_cancel=None,
            callback_progess=None,
            callback_done=api_rc._addon_params.callback_get_asset_done,
            force=True
        )


@dataclass
class ApiJobParamsGetAssets(ApiJobParams):
    """Get Assets specific parameters"""

    path_library: str
    tab: str  # KEY_TAB_ONLINE or KEY_TAB_MY_ASSETS
    category_list: List[str] = field(default_factory=lambda: ["All Assets"])
    search: str = ""
    idx_page: int = 1
    page_size: int = 10
    force_request: bool = False
    do_get_all: bool = True
    # Not exactly parameters, more results, may be used in callback_done
    already_in_index: bool = False
    asset_id_list: List[int] = field(default_factory=lambda: [])

    POOL_KEY = PoolKeys.INTERACTIVE

    def __eq__(self, other):
        return self.tab == other.tab and \
               self.category_list == other.category_list and \
               self.search == other.search and \
               self.idx_page == other.idx_page and \
               self.page_size == other.page_size and \
               self.force_request == other.force_request and \
               self.do_get_all == other.do_get_all

    def _get_key(self) -> str:
        """Returns search key for this job."""

        return get_search_key(self.tab, self.search, self.category_list)

    def _map_categories_local_to_api(self) -> List[str]:
        """Maps categories from local/plugin to API convention."""

        # Removing "All Assets" from categories. API expects only the another
        # categories when there are more than one in the list.
        to_map_list = self.category_list
        if len(self.category_list) > 1 and self.category_list[0] == CATEGORY_ALL:
            to_map_list = self.category_list[1:]

        mapped_category_list = [
            CATEGORY_MAPPING_LOCAL_TO_API.get(cat, cat)
            for cat in to_map_list
        ]
        return mapped_category_list

    def _get_algolia_query(self) -> Dict:
        """Returns a dict with Algolia search parameters."""

        if "Free" in self.category_list:
            numeric_filters = ["Credit=0"]
        else:
            numeric_filters = ["Credit>=0"]

        mapped_category_list = self._map_categories_local_to_api()
        category_root = mapped_category_list[0]

        if len(mapped_category_list) > 1:
            # TODO(Andreas): strange branch from P4B
            if category_root == CATEGORY_ALL:
                facet_filters = [[]]
                # for asset_type in ["HDRIs", "Models", "Textures"]:  # TODO(Andreas): have constant for ALL_TYPES
                #     if (
                #         f"/{mapped_category_list[1]}"
                #         in self.vCategories[self.tab][asset_type].keys()
                #     ):
                categories = [self.category_list[1]] + mapped_category_list[1:]
                level = len(categories) - 1
                categories = " > ".join(categories)
                facet_filters[0].append(
                    f"RefineCategories.lvl{level}:{categories}"
                )
            else:
                level = len(mapped_category_list) - 1
                categories = " > ".join(mapped_category_list)
                facet_filters = [f"RefineCategories.lvl{level}:{categories}"]
        elif category_root != CATEGORY_ALL:
            facet_filters = [f"RefineCategories.lvl0:{category_root}"]
        else:
            facet_filters = []

        query = {
            "query": self.search,
            "page": self.idx_page,
            "perPage": self.page_size,
            "algoliaParams": {"facetFilters": facet_filters,
                              "numericFilters": numeric_filters
                              },
        }
        return query

    def thread_execute(self, api_rc, job) -> None:
        """Executes a get assets job in a thread,
        started from thread_schedule.
        """

        key = self._get_key()
        api_rc.logger.debug(f"Cat/search change: {key}")

        query_exists = api_rc._asset_index.query_exists(
            key, self.idx_page, self.page_size)
        if query_exists and not self.force_request:
            self.already_in_index = True
            job.result = ApiResponse(ok=True,
                                     body={"data": []},
                                     error="job cancelled, query exists")
            return

        query = self._get_algolia_query()
        if self.tab == KEY_TAB_MY_ASSETS:
            job.result = api_rc._api.get_user_assets(query_data=query)
        else:
            job.result = api_rc._api.get_assets(query_data=query)

    def finish(self, api_rc, job) -> None:
        """Finishes get asset jobs, called from thread_collect."""

        if not job.result.ok or self.already_in_index:
            return

        key = self._get_key()

        api_rc.logger.debug(f"Cat/search change FINISHED: {key}")

        is_later_page = self.idx_page > 1
        asset_id_list = api_rc._asset_index.populate_assets(
            job.result, key, -1, 1000000, append_query=is_later_page)
        api_rc._asset_index.store_query(
            asset_id_list, key, self.idx_page, self.page_size)

        self.asset_id_list = asset_id_list
        for asset_id in asset_id_list:
            asset_name = api_rc._asset_index.get_asset_name(asset_id)
            dir_asset = os.path.join(self.path_library, asset_name)
            api_rc._asset_index.update_from_directory(
                asset_id,
                dir_asset,
                workflow_fallback="REGULAR"
            )
        job_is_done = self.idx_page >= job.result.body.get("last_page", -1)

        if not self.do_get_all or job_is_done:
            return

        api_rc.add_job_get_assets(
            path_library=self.path_library,
            tab=self.tab,
            category_list=self.category_list,
            search=self.search,
            idx_page=self.idx_page + 1,
            page_size=self.page_size,
            force_request=self.force_request,
            do_get_all=self.do_get_all,
            callback_cancel=None,
            callback_progess=None,
            callback_done=job.callback_done,
            force=True)


@dataclass
class ApiJobParamsDownloadThumb(ApiJobParams):
    """Download Thumb specific parameters"""

    asset_id: int
    url: str
    path: str
    do_update: bool = False
    skip_download: bool = False

    POOL_KEY = PoolKeys.PREVIEW_DL

    def __eq__(self, other):
        return self.url == other.url

    def thread_execute(self, api_rc, job) -> None:
        """Executes a download thumb job in a thread,
        started from thread_schedule.
        """

        try:
            self.skip_download = job.callback_progess(downloading=True,
                                                      job=job)
        except TypeError:
            self.skip_download = False

        if self.skip_download:
            job.result = ApiResponse(ok=True, body={}, error="")
            return

        path_download = f"{self.path}_dl"
        url_resized = api_rc._api.cdn_resize_img_url(self.url,
                                                     size=THUMB_SIZE_DOWNLOAD,
                                                     sharpen=1,  # TODO(Andreas): desired value?
                                                     quality=75,  # TODO(Andreas): desired value?
                                                     ext="auto")
        job.result = api_rc._api.download_preview(url_resized, path_download)
        # TODO(Andreas): error reporting,
        try:
            if os.path.exists(path_download):
                if os.path.exists(self.path):
                    os.remove(self.path)
                os.rename(path_download, self.path)
        except OSError:  # TODO(Andreas)
            api_rc.logger.exception(f"### rename exc: {self.path}")

    def finish(self, api_rc, job) -> None:
        """Finishes download thumb jobs, called from thread_collect."""

        if self.skip_download:
            return
        try:
            job.callback_progess(downloading=False, job=job)
        except TypeError:
            pass  # nothing to do


@dataclass
class ApiJobParamsPurchaseAsset(ApiJobParams):
    """Asset purchase specific parameters"""

    asset_data: AssetData
    category_list: List[str] = field(default_factory=lambda: ["All Assets"])
    search: str = ""
    # An optional follow-up download job
    job_download: Optional = None  # type ApiJob

    POOL_KEY = PoolKeys.INTERACTIVE

    def __eq__(self, other):
        return self.asset_data.asset_id == other.asset_data.asset_id

    def _get_category_slug(self) -> str:
        """Gets the slug format of the active category.
        E.g.:
        from ["All Models"] to "/"
        from ["Models", "Bathroom"] to "/models/bathroom"
        and undo transforms of f_GetCategoryChildren.
        """

        # TODO(related to SOFT-762 and SOFT-598):
        #      Refactor f_GetCategoryChildren as part of Core migration.
        category_slug = "/" + "/".join(
            [cat.lower().replace(" ", "-") for cat in self.category_list]
        )
        if category_slug.startswith("/hdris/"):
            category_slug = category_slug.replace("/hdris/", "/hdrs/")
        elif category_slug == "/all-assets":
            category_slug = "/"
        return category_slug

    def thread_execute(self, api_rc, job) -> None:
        """Executes a purchase asset job in a thread,
        started from thread_schedule.
        """

        asset_data = self.asset_data
        asset_id = asset_data.asset_id

        search = self.search.lower()
        category_slug = self._get_category_slug()

        asset_data.state.purchase.start()
        job.result = api_rc._api.purchase_asset(
            asset_id, search, category_slug)

    def finish(self, api_rc, job) -> None:
        """Finishes purchase asset jobs, called from thread_collect."""

        asset_data = self.asset_data
        asset_id = asset_data.asset_id
        if job.result.ok:
            api_rc._asset_index.mark_purchased(asset_id)
            if self.job_download is not None:
                api_rc.enqueue_job(self.job_download)
        else:
            api_rc.logger.error((f"Failed to purchase asset {asset_id}"
                                 f"\nerror: {job.result.error}"
                                 f"\nbody: {job.result.body}"))

            # TODO(Andreas): Show error
            # Check the reason for failure.
            # if "enough credits" in req.error:
            #     ui_err = DisplayError(
            #         asset_id=asset_id,
            #         asset_name=asset,
            #         button_label="Need credits",
            #         description=f"{req.error})"
            #     )
            # else:
            #     ui_err = DisplayError(
            #         asset_id=asset_id,
            #         asset_name=asset,
            #         button_label="Failed, retry",
            #         description=f"Error during purchase, please try again ({req.error})"
            #     )
        asset_data.state.purchase.end()

        api_rc._asset_index.flush_queries_by_tab(KEY_TAB_MY_ASSETS)

        # By requesting user data multiple follow up jobs get kicked off
        # (besides updating credits):
        # - Update category data
        # - Update online assets (under normal conditions already cached -> skipped)
        # - Update my_assets
        api_rc.add_job_get_user_data(
            api_rc._addon.user.user_name,
            api_rc._addon.user.user_id,
            callback_cancel=None,
            callback_progess=None,
            callback_done=api_rc._addon_params.callback_get_user_data_done,
            force=True
        )


@dataclass
class ApiJobParamsDownloadAsset(ApiJobParams):
    """Asset download specific parameters"""

    asset_data: AssetData
    size: str
    size_bg: str = ""
    type_bg: str = "EXR"
    lod: str = "NONE"
    variant: str = ""
    download_lods: bool = False
    native_mesh: bool = True
    renderer: str = ""

    POOL_KEY = PoolKeys.ASSET_DL

    def __eq__(self, other):
        return self.asset_data.asset_id == other.asset_data.asset_id and \
               self.size == other.size and \
               self.size_bg == other.size_bg and \
               self.type_bg == other.type_bg and \
               self.lod == other.lod and \
               self.variant == other.variant

    def thread_execute(self, api_rc, job) -> None:
        """Executes an asset download job in a thread,
        started from thread_schedule.
        """

        # Wrap to pass job through api
        try:
            partial_progress = partial(job.callback_progess, job)
        except TypeError:
            # No progress callback
            partial_progress = None

        result = api_rc._addon.download_asset_sync(
            self.asset_data,
            self.size,
            self.download_lods,
            self.native_mesh,
            self.renderer,
            update_callback=partial_progress
        )
        job.result = ApiResponse(ok=result,
                                 body={},
                                 error="")

    def finish(self, api_rc, job) -> None:
        """Finishes download asset jobs, called from thread_collect."""

        asset_data = self.asset_data
        asset_id = asset_data.asset_id
        error = asset_data.state.dl.error

        asset_data.state.dl.end()
        if job.result.ok:
            download_dir = asset_data.state.dl.get_directory()
            api_rc._asset_index.update_from_directory(asset_id, download_dir)
            if self.size != "NONE":
                asset_data.runtime.store_current_size(self.size)
            asset_data.state.dl.set_recently_downloaded(True)
        elif error not in [None, ""] and job.result.error != "":
            job.result.error = asset_data.state.dl.error


@dataclass
class ApiJobParamsDownloadWMPreview(ApiJobParams):
    """Asset download specific parameters"""

    asset_data: AssetData
    renderer: str = ""
    # Not exactly parameters, more results, may be used in callback_done
    files: List[str] = field(default_factory=lambda: [])

    POOL_KEY = PoolKeys.ASSET_DL

    def __eq__(self, other):
        return self.asset_data.asset_id == other.asset_data.asset_id

    def thread_execute(self, api_rc, job) -> None:
        """Executes an download watermarked preview job in a thread,
        started from thread_schedule.
        """

        path_wm_previews = api_rc._addon.get_wm_download_path(
            self.asset_data.asset_name)

        asset_type_data = self.asset_data.get_type_data()
        urls_wm = asset_type_data.get_watermark_preview_url_list()

        files_to_download = []
        if self.asset_data.is_backplate():
            # TODO(Andreas): Untested branch
            path_wm = os.path.join(path_wm_previews,
                                   self.asset_data.asset_name + "_WM.jpg_dl")
            if not os.path.exists(path_wm):
                files_to_download.append((urls_wm[0], path_wm))
        else:
            try:
                os.makedirs(path_wm_previews, exist_ok=True)
            except BaseException:
                # TODO(Andreas)
                api_rc.logger.exception("Failed to create dir for WM previews")

            for url in urls_wm:
                filename_wm_dl = os.path.basename(url.split("?")[0])
                filename_wm_dl += "_dl"
                # Might need to skip certain maps to improve performance
                # if any(vM in vFName for vM in ['BUMP','DISP']+[f'VAR{i}' for i in range(2,9)]) :
                #    continue

                path_wm = os.path.join(path_wm_previews, filename_wm_dl)
                if not os.path.exists(path_wm):
                    files_to_download.append((url, path_wm))

        if len(files_to_download) > 0:
            job.result = api_rc._addon.download_material_wm(files_to_download)
        else:
            job.result = ApiResponse(ok=True, body={}, error="")

    def finish(self, api_rc, job) -> None:
        """Finishes download asset jobs, called from thread_collect."""

        path_wm_previews = api_rc._addon.get_wm_download_path(
            self.asset_data.asset_name)
        asset_id = self.asset_data.asset_id
        api_rc._asset_index.update_from_directory(
            asset_id, path_wm_previews, workflow_fallback="METALNESS")
