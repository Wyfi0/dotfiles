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

"""Module for general purpose updating for Poliigon software."""
from typing import Dict, Optional, Sequence, Tuple, Callable, Any
from dataclasses import dataclass
import datetime
import json
import os
import threading
import requests

from .notifications import (Notification,
                            NotificationSystem,
                            NOTICE_TITLE_UPDATE)


BASE_URL = "https://software.poliigon.com"
TIMEOUT = 20.0

# Status texts
FAIL_GET_VERSIONS = "Failed to get versions"


def v2t(value: str) -> tuple:
    """Take a version string like v1.2.3 and convert it to a tuple."""
    if not value or "." not in value:
        return None
    if value.lower().startswith("v"):
        value = value[1:]
    return tuple([int(ind) for ind in value.split(".")])


def t2v(ver: tuple) -> str:
    """Take a tuple like (2, 80) and construct a string like v2.80."""
    return "v" + ".".join(list(ver))


@dataclass
class AlertData:
    title: Optional[str] = None
    label: Optional[str] = None
    body: Optional[str] = None
    url: Optional[str] = None
    priority: Optional[int] = None
    action_string: Optional[str] = None
    open_popup: Optional[bool] = None
    allow_dismiss: bool = True
    auto_dismiss: bool = True

    valid: bool = True

    def validate_field(
            self,
            value: Any,
            field: str,
            field_type: type,
            mandatory: bool,
            report_callable: Optional[Callable] = None):
        exists = value is not None
        ok_exists = (exists or not mandatory)
        ok_type = True
        if exists:
            ok_type = type(value) == field_type
        if not ok_exists or not ok_type:
            self.valid = False
            if report_callable is not None:
                report_callable(
                    "invalid_alert_information",
                    f"Invalid {field} {value}",
                    "error")

    def validate_data(self, report_callable: Optional[Callable] = None) -> bool:
        rc = report_callable
        self.validate_field(self.title, "Title", str, True, rc)
        self.validate_field(self.label, "Label", str, True, rc)
        self.validate_field(self.priority, "Priority", int, True, rc)

        self.validate_field(self.url, "Url", str, False, rc)
        self.validate_field(self.action_string, "Action String", str, False, rc)

        self.validate_field(self.auto_dismiss, "Auto Dismiss", bool, False, rc)
        self.validate_field(self.allow_dismiss, "Allow Dismiss", bool, False, rc)

        if self.url is None:
            # one of url or body have to be available with right format
            self.validate_field(self.body, "Body", str, True, rc)

    def update_from_dict(
            self, data: Dict, report_callable: Optional[Callable] = None) -> None:
        self.title = data.get("title")
        self.label = data.get("label")
        self.body = data.get("body")
        self.url = data.get("url")
        self.action_string = data.get("action_string")
        self.priority = data.get("priority")

        self.allow_dismiss = data.get("allow_dismiss", True)
        self.auto_dismiss = data.get("auto_dismiss", True)

        if self.url is None or self.url == "":
            self.open_popup = True

        self.validate_data(report_callable)

    def create_notification(
            self, notification_system: NotificationSystem) -> Optional[Notification]:
        if not self.valid:
            return None
        notice = notification_system.create_version_alert(
            title=self.title,
            priority=self.priority,
            label=self.label,
            tooltip=self.label,
            body=self.body,
            action_string=self.action_string,
            url=self.url,
            open_popup=self.open_popup,
            allow_dismiss=self.allow_dismiss,
            auto_dismiss=self.auto_dismiss
        )

        return notice


@dataclass
class VersionData:
    """Container for a single version of the software."""
    version: Optional[tuple] = None
    url: Optional[str] = None
    min_software_version: Optional[tuple] = None  # Inclusive.
    max_software_version: Optional[tuple] = None  # Not inclusive.
    required: Optional[bool] = None
    release_timestamp: Optional[datetime.datetime] = None
    alert: Optional[AlertData] = None

    # Internal, huamn readable current status.
    status_title: str = ""
    status_details: str = ""
    status_ok: bool = True

    # Reporting rate for the version
    error_sample_rate: Optional[float] = None
    traces_sample_rate: Optional[float] = None

    def update_from_dict(
            self, data: Dict, report_callable: Optional[Callable] = None) -> None:
        self.version = v2t(data.get("version"))
        self.url = data.get("url", "")

        # List format like [2, 80]
        self.min_software_version = tuple(data.get("min_software_version"))
        self.max_software_version = tuple(data.get("max_software_version"))
        self.required = data.get("required")
        self.release_timestamp = data.get("release_timestamp")

        alert_data = data.get("alert", None)
        if alert_data is not None:
            self.alert = AlertData()
            self.alert.update_from_dict(alert_data, report_callable)

        self.error_sample_rate = (data.get("error_sample_rate"))
        self.traces_sample_rate = (data.get("traces_sample_rate"))

    def create_alert_notification(
            self, notification_system: NotificationSystem) -> Optional[Notification]:

        if self.alert is None or notification_system is None:
            return
        return self.alert.create_notification(notification_system)

    def create_update_notification(
            self, notification_system: NotificationSystem) -> Optional[Notification]:
        if self.url is None or notification_system is None:
            return

        label = f"{NOTICE_TITLE_UPDATE} {self.version}"
        notice = notification_system.create_update(
            tooltip=NOTICE_TITLE_UPDATE,
            label=label,
            download_url=self.url
        )

        return notice


class SoftwareUpdater():
    """Primary class which implements checks for updates and installs."""

    # Versions of software available.
    stable: Optional[VersionData]
    latest: Optional[VersionData]
    all_versions: Sequence

    # Always initialized
    addon_name: str  # e.g. poliigon-addon-blender.
    addon_version: tuple  # Current addon version.
    software_version: tuple  # DCC software version, e.g. (3, 0).
    base_url: str  # Primary url where updates and version data is hosted.

    # State properties.
    update_ready: Optional[bool] = None  # None until proven true or false.
    update_data: Optional[VersionData] = None
    _last_check: Optional[datetime.datetime] = None
    last_check_callback: Optional[Callable] = None  # When last_check changes.
    check_interval: Optional[int] = None  # interval in seconds between auto check.
    verbose: bool = True

    # Classes to be imported from the addon
    notification_system: Optional[NotificationSystem] = None
    reporting_callable: Optional[Callable] = None

    # Notifications
    alert_notice: Optional[Notification] = None
    update_notice: Optional[Notification] = None

    # Bool value to be set by addon to take the update
    # data from the latest version instead of the stable one
    update_from_latest: bool = False

    _check_thread: Optional[threading.Thread] = None

    def __init__(self,
                 addon_name: str,
                 addon_version: tuple,
                 software_version: tuple,
                 base_url: Optional[str] = None,
                 notification_system: Optional[NotificationSystem] = None,
                 local_json: Optional[str] = None):
        self.addon_name = addon_name
        self.addon_version = addon_version
        self.notification_system = notification_system
        self.software_version = software_version
        self.base_url = base_url if base_url is not None else BASE_URL
        self.local_json = local_json
        self.current_version = VersionData()

    @property
    def is_checking(self) -> bool:
        """Interface for other modules to see if a check for update running."""
        return self._check_thread and self._check_thread.is_alive()

    @property
    def last_check(self) -> str:
        if not self._last_check:
            return ""
        try:
            return self._last_check.strftime("%Y-%m-%d %H:%M")
        except ValueError as err:
            print("Get last update check error:", err)
            return ""

    @last_check.setter
    def last_check(self, value: str) -> None:
        try:
            self._last_check = datetime.datetime.strptime(
                value, "%Y-%m-%d %H:%M")
        except ValueError as err:
            print("Assign last update check error:", value, err)
            print(err)
            self._last_check = None
        if self.last_check_callback:
            self.last_check_callback(self.last_check)  # The string version.

    def _clear_versions(self) -> None:
        self.stable = None
        self.latest = None
        self.all_versions = []

    def _clear_update(self) -> None:
        self.update_ready = None  # Set to None until proven true or false.
        self.update_data = None
        self.status_ok = True

    def has_time_elapsed(self, hours: int = 24) -> bool:
        """Checks if a given number of hours have passed since last check."""
        now = datetime.datetime.now()
        if not self._last_check:
            return True  # No check on record.
        diff = now - self._last_check
        return diff.total_seconds() / 3600.0 > hours

    def print_debug(self, *args):
        if self.verbose:
            print(*args)

    def update_versions(self) -> None:
        """Fetch the latest versions available from the server."""
        self.status_ok = True  # True until proven false.
        self._clear_versions()
        url = f"{self.base_url}/{self.addon_name}-versions.json"

        try:
            res = requests.get(url, timeout=TIMEOUT)
        except requests.exceptions.ConnectionError:
            self.status_title = FAIL_GET_VERSIONS
            self.status_ok = False
            self.status_details = "Updater ConnectionError"
            return
        except requests.exceptions.Timeout:
            self.status_title = FAIL_GET_VERSIONS
            self.status_ok = False
            self.status_details = "Updater Timeout"
            return
        except requests.exceptions.ProxyError:
            self.status_title = FAIL_GET_VERSIONS
            self.status_ok = False
            self.status_details = "Updater ProxyError"
            return

        if not res.ok:
            self.status_title = FAIL_GET_VERSIONS
            self.status_details = (
                "Did not get OK response while fetching available versions "
                f"from {url}")
            self.status_ok = False
            print(self.status_details)
            return
        if res.status_code != 200:
            self.status_title = FAIL_GET_VERSIONS
            self.status_details = (
                "Did not get OK code while fetching available versions")
            self.status_ok = False
            print(self.status_details)
            return

        try:
            resp = json.loads(res.text)
            if self.local_json is not None and os.path.isfile(self.local_json):
                with open(self.local_json) as f:
                    resp = json.load(f)
        except json.decoder.JSONDecodeError as e:
            self.status_title = FAIL_GET_VERSIONS
            self.status_details = "Could not parse json response for versions"
            self.status_ok = False
            self.status_is_error = True
            print(self.status_details)
            print(e)
            return

        if resp.get("stable"):
            self.stable = VersionData()
            self.stable.update_from_dict(resp["stable"])
        if resp.get("latest"):
            self.latest = VersionData()
            self.latest.update_from_dict(resp["latest"])
        if resp.get("versions"):
            for itm in resp["versions"]:
                ver = VersionData()
                ver.update_from_dict(itm, self.reporting_callable)
                self.all_versions.append(ver)
                if ver.version == self.addon_version:
                    self.current_version = ver

        self._last_check = datetime.datetime.now()
        self.last_check = self.last_check  # Trigger callback.

    def _update_notification_msg(self) -> None:
        if self.notification_system is None:
            return

        body = self.notification_system.addon_params.update_body
        if self.update_data is not None and body is not None:
            self.notification_system.addon_params.update_body = body.format(
                self.update_data.version)

    def _create_notifications(self) -> Tuple[Notification, Notification]:
        alert_notif = None
        update_notif = None
        self._update_notification_msg()
        if self.current_version.alert is not None:
            alert_notif = self.current_version.create_alert_notification(
                self.notification_system)
        if self.update_data is not None:
            update_notif = self.update_data.create_update_notification(
                self.notification_system)
        return update_notif, alert_notif

    def check_for_update(self,
                         callback: Optional[callable] = None,
                         create_notifications: bool = False) -> bool:
        """Fetch and check versions to see if a new update is available."""
        self._clear_update()
        self.update_versions()

        if not self.status_ok:
            if callback:
                callback()
            return False

        # First compare against latest
        if self.stable and self._check_eligible(self.stable):
            update_version = self.stable
            if self.update_from_latest:
                update_version = self.latest

            self.print_debug(
                "Using latest stable:",
                update_version.version,
                "vs current addon: ",
                self.addon_version)

            if update_version.version > self.addon_version:
                self.update_data = update_version
                self.update_ready = True
            else:
                self.update_ready = False
            if create_notifications and self.notification_system is not None:
                self.update_notice, self.alert_notice = self._create_notifications()
            if callback:
                callback()
            return True

        # Eligible wasn't present or more eligible, find next best.
        self.print_debug("Unable to use current stable release")
        max_version = self.get_max_eligible()
        if max_version:
            if max_version.version > self.addon_version:
                self.update_data = max_version
                self.update_ready = True
            else:
                self.update_ready = False
        else:
            self.print_debug("No eligible releases found")
            self.update_ready = False

        if create_notifications and self.notification_system is not None:
            self.update_notice, self.alert_notice = self._create_notifications()
        if callback is not None:
            callback()
        return True

    def _check_eligible(self, version: VersionData) -> bool:
        """Verify if input version is compatible with the current software."""
        eligible = True
        if version.min_software_version:
            if self.software_version < version.min_software_version:
                eligible = False
        elif version.max_software_version:
            # Inclusive so that if max is 3.0, must be 2.99 or lower.
            if self.software_version >= version.max_software_version:
                eligible = False
        return eligible

    def get_max_eligible(self) -> Optional[VersionData]:
        """Find the eligible version with the highest version number."""
        max_eligible = None
        for ver in self.all_versions:
            if not self._check_eligible(ver):
                continue
            elif max_eligible is None:
                max_eligible = ver
            elif ver.version > max_eligible.version:
                max_eligible = ver
        return max_eligible

    def async_check_for_update(
            self, callback: Callable = None, create_notifications: bool = False):
        """Start a background thread which will check for updates."""

        if self.is_checking:
            return

        self._check_thread = threading.Thread(
            target=self.check_for_update,
            args=(callback, create_notifications))

        self._check_thread.daemon = True
        self._check_thread.start()
