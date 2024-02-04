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

"""Module fo asynchronous user notificatiions."""

from dataclasses import dataclass, field
from functools import wraps
from enum import IntEnum
from queue import Queue
from threading import Lock
from typing import Callable, Dict, List, Optional, Any

from .thread_manager import PoolKeys

# Predefined priority values (lower numbers -> higher prio)
NOTICE_PRIO_LOWEST = 200
NOTICE_PRIO_LOW = 100
NOTICE_PRIO_MEDIUM = 50
NOTICE_PRIO_HIGH = 20
NOTICE_PRIO_URGENT = 1

NOTICE_PRIO_MAT_TEMPLATE = NOTICE_PRIO_HIGH
NOTICE_PRIO_NO_INET = NOTICE_PRIO_LOW  # Show, but other errors have precedent
NOTICE_PRIO_PROXY = NOTICE_PRIO_MEDIUM
NOTICE_PRIO_SETTINGS_WRITE = NOTICE_PRIO_HIGH
NOTICE_PRIO_SURVEY = NOTICE_PRIO_LOWEST
NOTICE_PRIO_UPDATE = NOTICE_PRIO_HIGH + 5  # urgent, but room for "more urgent"

# Predefined notice IDs
NOTICE_ID_MAT_TEMPLATE = "MATERIAL_TEMPLATE_ERROR"
NOTICE_ID_NO_INET = "NO_INTERNET_CONNECTION"
NOTICE_ID_PROXY = "PROXY_CONNECTION_ERROR"
NOTICE_ID_SETTINGS_WRITE = "SETTINGS_WRITE_ERROR"
NOTICE_ID_SURVEY_FREE = "NPS_INAPP_FREE"
NOTICE_ID_SURVEY_ACTIVE = "NPS_INAPP_ACTIVE"
NOTICE_ID_UPDATE = "UPDATE_READY_MANUAL_INSTALL"
NOTICE_ID_VERSION_ALERT = "ADDON_VERSION_ALERT"

# Predefined notice titles
# Used a default param in create functions, but should usually be overridden
# by passing in localized titles from DCC.
NOTICE_TITLE_MAT_TEMPLATE = "Material template error"
NOTICE_TITLE_NO_INET = "No internet access"
NOTICE_TITLE_PROXY = "Encountered proxy error"
NOTICE_TITLE_SETTINGS_WRITE = "Failed to write settings"
NOTICE_TITLE_SURVEY = "How's the addon?"
NOTICE_TITLE_UPDATE = "Update ready"
NOTICE_TITLE_DEPRECATED = "Deprecated version"

# Predefined icons, assign DCC specific key/reference via init_icons()
NOTICE_ICON_WARN = "ICON_WARN"
NOTICE_ICON_INFO = "ICON_INFO"
NOTICE_ICON_SURVEY = "ICON_SURVEY"


class ActionType(IntEnum):
    # Note: Numerical values are still same as in P4B, but entries got
    #       sorted alphabetically
    OPEN_URL = 1
    POPUP_MESSAGE = 3
    RUN_OPERATOR = 4
    UPDATE_READY = 2


class SignalType(IntEnum):
    """Types of each interaction with the notifications."""

    VIEWED = 0
    DISMISSED = 1
    CLICKED = 2


@dataclass
class Notification():
    """Container object for a user notification.

    NOTE: Do not instance Notification directly, but instead either use
          NotificationSystem.create_... functions or instance derived
          NotificationXYZ classes.
    """

    # Unique id for this specific kind of notice, if possible re-use above
    # NOTICE_ID_xyz.
    id_notice: str
    # Main title, should be short
    title: str
    # Indicator of how to structure and draw notification.
    action: ActionType = field(init=False)  # does NOT get auto initialized
    # Priority is always > 0, lower values = higher priority
    priority: int
    # Label to be shown in the notification banner - to be defined on each addon
    label: str = ""
    # Allow the user to dismiss the notification.
    allow_dismiss: bool = True
    # Dismiss after user interacted with the notification
    auto_dismiss: bool = False
    # Hover-over tooltip, if there is a button
    tooltip: str = ""
    # In Blender icon's are referenced in strings (icon enum),
    # but this may differ per DCC. For prebuilt notices init_icons() has
    # to be used to store DCC dependent icons, once.
    icon: Optional[any] = None
    # Defines if notification has to open a popup with more information and
    # options for the user to then address or dismiss the notice
    open_popup: bool = False
    # Text for the button to execute notify callable when it opens a popup
    action_string: Optional[str] = None
    # action callable to be attached to the notification, to be executed
    # when notification is clicked
    action_callable: Optional[Callable] = None
    # function to be called when the notification is dismissed (viewed or not)
    on_dismiss_callable: Optional[Callable] = None

    viewed: bool = False  # False until actually drawn
    clicked: bool = False  # False until user interact with the notice


@dataclass
class AddonNotificationsParameters:
    """Parameters to be parsed from the addon.

    parameters:
    update_callable: Callable to be set as action_callable to update notifications
    update_action_text: Action text for updates - used as popup update button text
    update_body: Text with a description for update - used as popup text

    NOTE: Feel free to add here any other parameter needed from the addon.
    """

    update_callable: Optional[Callable] = None
    update_action_text: str = NOTICE_TITLE_UPDATE
    update_body: str = ""

@dataclass
class NotificationOpenUrl(Notification):
    url: str = ""

    def __post_init__(self):
        self.action = ActionType.OPEN_URL

    def get_key(self) -> str:
        return "".join([self.action.name, self.url, self.label])


@dataclass
class NotificationPopup(Notification):
    body: str = ""
    url: str = ""
    alert: bool = True

    def __post_init__(self):
        self.action = ActionType.POPUP_MESSAGE

    def get_key(self) -> str:
        return "".join([self.action.name, self.url, self.body])


@dataclass
class NotificationRunOperator(Notification):
    # For Blender ops_name will be string, for C4D not so sure, yet...
    # I guess, we could even store a callable in here.
    ops_name: Optional[any] = None

    def __post_init__(self):
        self.action = ActionType.RUN_OPERATOR

    def get_key(self) -> str:
        return "".join([self.action.name, self.ops_name])


@dataclass
class NotificationUpdateReady(Notification):
    download_url: str = ""
    download_label: str = ""
    logs_url: str = ""
    logs_label: str = ""
    body: str = ""

    def __post_init__(self):
        self.action = ActionType.UPDATE_READY

    def get_key(self) -> str:
        return "".join([self.action.name, self.download_url, self.download_label])


class NotificationSystem():
    """Abstraction to handle asynchronous user notification.

    Each DCC has to populate icon_dcc_map.
    """

    _api = None  # PoliigonConector
    _tm = None  # Thread Manager

    _queue_notice: Queue = Queue()
    _lock_notice: Lock = Lock()
    _notices: Dict = {}  # {key: Notification}

    # Each DCC use init_icons() to populate these values as is fitting for
    # themselves.
    icon_dcc_map: Dict[str, Optional[any]] = {
        NOTICE_ICON_WARN: None,
        NOTICE_ICON_INFO: None,
        NOTICE_ICON_SURVEY: None
    }

    addon_params = AddonNotificationsParameters()

    def __init__(self, addon):
        if addon is None:
            return
        self._api = addon._api
        self._tm = addon._tm

    def init_icons(
            self,
            icon_warn: Optional[Any] = None,
            icon_info: Optional[Any] = None,
            icon_survey: Optional[Any] = None) -> None:
        self.icon_dcc_map[NOTICE_ICON_WARN] = icon_warn
        self.icon_dcc_map[NOTICE_ICON_INFO] = icon_info
        self.icon_dcc_map[NOTICE_ICON_SURVEY] = icon_survey

    def _run_threaded(key_pool: PoolKeys,
                      max_threads: Optional[int] = None,
                      foreground: bool = False) -> Callable:
        """Schedules a function to run in a thread of a chosen pool."""

        def wrapped_func(func: Callable) -> Callable:
            @wraps(func)
            def wrapped_func_call(self, *args, **kwargs):
                args = (self, ) + args
                return self._tm.queue_thread(func,
                                             key_pool,
                                             max_threads,
                                             foreground,
                                             *args,
                                             **kwargs)
            return wrapped_func_call
        return wrapped_func

    def _consume_queued_notices(self) -> None:
        """Empties the notice queue and stores all new notices in _notices.

        Note: If an identical notice already exists, it will get skipped.
        """

        with self._lock_notice:
            while self._queue_notice.qsize() > 0:
                notice = self._queue_notice.get(block=False)
                key = notice.get_key()
                if key in self._notices:
                    continue
                self._notices[key] = notice

    def _get_sorted_notices(self) -> List[Notification]:
        """Returns a priority sorted list with all notices."""

        with self._lock_notice:
            all_notices = list(self._notices.values())
        all_notices.sort(key=lambda notice: notice.priority)
        return all_notices

    @_run_threaded(PoolKeys.INTERACTIVE)
    def _thread_signal(
            self, notice: Notification, signal_type: SignalType) -> None:
        """Asynchronously signals "notice got viewed" to server"."""

        if signal_type == SignalType.VIEWED:
            self._api.signal_view_notification(notice.id_notice)
        elif signal_type == SignalType.DISMISSED:
            self._api.signal_dismiss_notification(notice.id_notice)
        elif signal_type == SignalType.CLICKED:
            self._api.signal_click_notification(notice.id_notice, notice.action)

    def _signal_view(self, notice: Notification):
        """Internally used to start the signal view thread."""

        if self._api is None or not self._api._is_opted_in():
            return
        self._thread_signal(notice, SignalType.VIEWED)

    def _signal_clicked(self, notice: Notification):
        """Internally used to start the signal click thread."""

        if self._api is None or not self._api._is_opted_in():
            return
        self._thread_signal(notice, SignalType.CLICKED)

    def _signal_dismiss(self, notice: Notification):
        """Internally used to start the signal dismiss thread."""

        if self._api is None or not self._api._is_opted_in():
            return
        self._thread_signal(notice, SignalType.DISMISSED)

    def enqueue_notice(self, notice: Notification) -> None:
        """Enqueues a new notification."""

        self._queue_notice.put(notice)

    def dismiss_notice(
            self, notice: Notification, force: bool = False) -> None:
        """Dismisses a notice.

        Use force parameter to to dismiss 'un-dismissable' notices, e.g.
        a 'no internet' notice, when internet is back on.
        """

        if not notice.allow_dismiss and not force:
            return

        if not notice.clicked:
            self._signal_dismiss(notice)

        if notice.on_dismiss_callable is not None:
            notice.on_dismiss_callable()

        key = notice.get_key()
        with self._lock_notice:
            del self._notices[key]

    def clicked_notice(self, notice: Notification) -> None:
        """To be called, when a user interacted with the notice."""

        notice.clicked = True
        self._signal_clicked(notice)

        if notice.action_callable is not None:
            notice.action_callable()

        if not notice.auto_dismiss:
            return
        self.dismiss_notice(notice)

    def get_all_notices(self) -> List[Notification]:
        """Returns a priority sorted list with all notices.

        Usually called from draw code.
        """

        self._consume_queued_notices()
        return self._get_sorted_notices()

    def get_top_notice(
            self, do_signal_view: bool = False) -> Optional[Notification]:
        """Returns current highest priority notice.

        Usually called from draw code.
        """

        notices_by_prio = self.get_all_notices()
        try:
            notice = notices_by_prio[0]
            if do_signal_view and not notice.viewed:
                self._signal_view(notice)
                notice.viewed = True
        except (KeyError, IndexError):
            notice = None

        return notice

    def notification_popup(
            self, notice: Notification, do_signal_view: bool = False):
        """Called when a popup notification is drawn"""

        if do_signal_view and not notice.viewed:
            self._signal_view(notice)
        notice.viewed = True

    def flush_all(self):
        """Flushes all existing notices."""

        while not self._queue_notice.empty():
            self._queue_notice.get(block=False)

        with self._lock_notice:
            self._notices = {}

    def create_no_internet(self,
                           title: str = NOTICE_TITLE_NO_INET,
                           *,
                           tooltip: str,
                           body: str,
                           auto_enqueue: bool = True
                           ) -> Notification:
        """Returns a pre-built 'No internet' notice."""

        notice = NotificationPopup(
            id_notice=NOTICE_ID_NO_INET,
            title=title,
            priority=NOTICE_PRIO_NO_INET,
            allow_dismiss=False,
            tooltip=tooltip,
            icon=self.icon_dcc_map[NOTICE_ICON_WARN],
            body=body
        )
        if auto_enqueue:
            self.enqueue_notice(notice)
        return notice

    def create_proxy(self,
                     title: str = NOTICE_TITLE_PROXY,
                     *,
                     tooltip: str,
                     body: str,
                     auto_enqueue: bool = True
                     ) -> Notification:
        """Returns a pre-built 'Proxy error' notice."""

        notice = NotificationPopup(
            id_notice=NOTICE_ID_PROXY,
            title=title,
            priority=NOTICE_PRIO_PROXY,
            allow_dismiss=False,
            tooltip=tooltip,
            icon=self.icon_dcc_map[NOTICE_ICON_WARN],
            body=body
        )
        if auto_enqueue:
            self.enqueue_notice(notice)
        return notice

    def create_survey(self,
                      title: str = NOTICE_TITLE_SURVEY,
                      *,
                      is_free_user: bool,
                      tooltip: str,
                      free_survey_url: str,
                      active_survey_url: str,
                      label: str,
                      auto_enqueue: bool = True,
                      on_dismiss_callable: Optional[Callable] = None
                      ) -> Notification:
        """Returns a pre-built 'user survey' notice."""

        if is_free_user:
            id_notice = NOTICE_ID_SURVEY_FREE
            url = free_survey_url
        else:
            id_notice = NOTICE_ID_SURVEY_ACTIVE
            url = active_survey_url
        notice = NotificationOpenUrl(
            id_notice=id_notice,
            title=title,
            priority=NOTICE_PRIO_SURVEY,
            allow_dismiss=True,
            auto_dismiss=True,
            tooltip=tooltip,
            url=url,
            label=label,
            icon=self.icon_dcc_map[NOTICE_ICON_SURVEY],
            on_dismiss_callable=on_dismiss_callable
        )
        if auto_enqueue:
            self.enqueue_notice(notice)
        return notice

    def create_write_mat_template(self,
                                  title: str = NOTICE_TITLE_MAT_TEMPLATE,
                                  *,
                                  tooltip: str,
                                  body: str,
                                  auto_enqueue: bool = True
                                  ) -> Notification:
        """Returns a pre-built 'Material template error' notice."""

        notice = NotificationPopup(
            id_notice=NOTICE_ID_MAT_TEMPLATE,
            title=title,
            priority=NOTICE_PRIO_MAT_TEMPLATE,
            allow_dismiss=True,
            tooltip=tooltip,
            icon=self.icon_dcc_map[NOTICE_ICON_WARN],
            body=body,
            alert=True
        )
        if auto_enqueue:
            self.enqueue_notice(notice)
        return notice

    def create_version_alert(self,
                             title: str = NOTICE_TITLE_DEPRECATED,
                             *,
                             priority: int,
                             label: str,
                             tooltip: str,
                             open_popup: bool,
                             allow_dismiss: bool = True,
                             auto_dismiss: bool = True,
                             body: Optional[str] = None,
                             action_string: Optional[str] = None,
                             url: Optional[str] = None,
                             auto_enqueue: bool = True
                             ) -> Notification:
        """Returns a pre-built 'Version Alert' notice.

        Note: An Alert Notification can be a NotificationPopup or a
        NotificationOpenUrl, depending on the given AlertData
        """

        if open_popup:
            notice = NotificationPopup(
                id_notice=NOTICE_ID_VERSION_ALERT,
                title=title,
                priority=priority,
                allow_dismiss=allow_dismiss,
                auto_dismiss=auto_dismiss,
                tooltip=tooltip,
                label=label,
                icon=self.icon_dcc_map[NOTICE_ICON_WARN],
                open_popup=open_popup,
                body=body,
                action_string=action_string
            )
        else:
            notice = NotificationOpenUrl(
                id_notice=NOTICE_ID_VERSION_ALERT,
                url=url,
                title=title,
                priority=priority,
                allow_dismiss=allow_dismiss,
                auto_dismiss=auto_dismiss,
                tooltip=tooltip,
                label=label,
                icon=self.icon_dcc_map[NOTICE_ICON_WARN]
            )

        if auto_enqueue:
            self.enqueue_notice(notice)
        return notice

    def create_write_settings_error(self,
                                    title: str = NOTICE_TITLE_SETTINGS_WRITE,
                                    *,
                                    tooltip: str,
                                    body: str,
                                    auto_enqueue: bool = True
                                    ) -> Notification:
        """Returns a pre-built 'write settings error' notice."""

        notice = NotificationPopup(
            id_notice=NOTICE_ID_SETTINGS_WRITE,
            title=title,
            priority=NOTICE_PRIO_SETTINGS_WRITE,
            allow_dismiss=True,
            tooltip=tooltip,
            icon=self.icon_dcc_map[NOTICE_ICON_WARN],
            body=body,
            alert=True
        )
        if auto_enqueue:
            self.enqueue_notice(notice)
        return notice

    def create_update(self,
                      title: str = NOTICE_TITLE_UPDATE,
                      *,
                      tooltip: str,
                      label: str,
                      download_url: str,
                      download_label: str = "",
                      logs_url: str = "",
                      logs_label: str = "",
                      auto_enqueue: bool = True,
                      open_popup: bool = True,
                      auto_dismiss: bool = True,
                      action_string: Optional[str] = None,
                      body: Optional[str] = None,
                      action_callable: Optional[Callable] = None
                      ) -> Notification:
        """Returns a pre-built 'Update available' notice."""

        if action_string is None:
            action_string = self.addon_params.update_action_text
        if body is None:
            body = self.addon_params.update_body
        if action_callable is None:
            action_callable = self.addon_params.update_callable

        notice = NotificationUpdateReady(
            id_notice=NOTICE_ID_UPDATE,
            title=title,
            priority=NOTICE_PRIO_UPDATE,
            allow_dismiss=True,
            auto_dismiss=auto_dismiss,
            tooltip=tooltip,
            download_url=download_url,
            download_label=download_label,
            label=label,
            logs_url=logs_url,
            logs_label=logs_label,
            icon=self.icon_dcc_map[NOTICE_ICON_INFO],
            open_popup=open_popup,
            action_string=action_string,
            body=body,
            action_callable=action_callable
        )
        if auto_enqueue:
            self.enqueue_notice(notice)
        return notice
