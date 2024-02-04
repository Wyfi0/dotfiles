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

"""This module establishes the mechanisms for exception and error reporting."""

import os
import platform
import re
import sys
import traceback
from typing import Optional

from .modules.poliigon_core.env import PoliigonEnvironment

import bpy

# Must add to local path due to sentry_sdk self importing.
base = os.path.dirname(__file__)
module_dir = os.path.join(base, "modules")
sys.path.append(module_dir)

import sentry_sdk

# Flag for whether to send events.
IS_OPTED_IN = True

# Container during an exception to allow user to add information to report.
LAST_ERROR_CONTEXT = None

# Draw error cache to avoid duplicate draw code reports.
DRAW_ERROR_CACHE = []

# From: https://develop.sentry.dev/sdk/event-payloads/transaction/
TRANSACT_OK = "ok"
TRANSACT_CANCEL = "cancelled"
TRANSACT_FAIL = "internal_error"

# Maximum number of characters sentry allows for a single tag body.
MAX_TAG_CHARS = 508


def initialize_sentry(software_name: str,
                      software_version: str,
                      tool_version: str,
                      env: PoliigonEnvironment
) -> None:
    """Set up sentry for exception capturing.

    Args:
        software_name: The name of this 3d software, e.g. blender
        software_version: This 3D software's version: 1.2.3, no prefix v
        tool_version: This plugin's version: 1.2.3 (no prefix v)
        env: Instance of class, where env.env_name is one of dev or prod.
    """
    if env.env_name not in ["dev", "prod", "test"]:
        raise ValueError(f"Environment must one of dev or prod, not {env}")

    sentry_sdk.init(
        "https://85cb67c748a041c9ad9b71bbd42f871d@o1010513.ingest.sentry.io/6345476",

        # Persistent setup.
        environment=env.env_name,  # "dev", "test", or "prod"
        release=tool_version,  # should be in form of v1.2.3

        # Don't send the users machine name as the hostname
        server_name='',

        # Don't perform automatic stack tracing, force using a wrapper instead.
        default_integrations=False,
        auto_enabling_integrations=False,
        auto_session_tracking=False,  # Applies to WSGI middleware, n/a here.

        # Set the sample rate for error reporting, where 1.0 would equate to
        # 100% of errors being reported. Review/update "Sentry for Software"
        # internal design doc before adjusting.
        sample_rate=1.0 if env.forced_sampling else 0.035,

        # Set the proportion of transactions to sample, where 1.0 would equate
        # to 100% of transactions for performance monitoring. Review/update
        # the "Sentry for Software" internal design doc before adjusting.
        traces_sample_rate=1.0 if env.forced_sampling else 0.01,
    )

    os_lower = platform.platform().lower()
    if "linux" in os_lower:
        os_name = "linux"
    elif "windows" in os_lower:
        os_name = "windows"
    elif "darwin" in os_lower or "macos" in os_lower:
        os_name = "mac"
    else:
        os_name = platform.platform()

    sentry_sdk.set_tag("software_name", software_name)
    sentry_sdk.set_tag("software_version", software_version)
    sentry_sdk.set_tag("release", tool_version)
    sentry_sdk.set_tag("os_name", os_name)
    sentry_sdk.set_tag("os_version", platform.platform())


def _is_foreground() -> bool:
    """Only send reports if in foreground mode."""
    return not bpy.app.background


def assign_user(userid: Optional[int]) -> None:
    """Update reporting to be associated to this logged in user."""
    if userid is None:
        sentry_sdk.set_user(None)
    else:
        sentry_sdk.set_user({"id": userid})


def set_optin(optin: bool) -> None:
    """Change whether reporting should be sent or not.

    End any current session on opt out, but don't immediately start a session
    if opting in - sessions are wrapped around individual operator calls.

    This function is triggered on startup to be in sync with user preferences.
    """
    global IS_OPTED_IN
    if IS_OPTED_IN:
        _flush()
    IS_OPTED_IN = bool(optin) and _is_foreground()


def get_optin() -> bool:
    """Returns current status, a function that may be injected elsewhere."""
    return IS_OPTED_IN


def handle_operator(silent=False):
    """Decorator for the execute(self, context) function of bpy operators.

    Captures any errors for user reporting, and triggers a popup if the wrapper
    is not set to silent, to give the user a chance to share details.

    Note: This dectorator has to be made seperate from the general purpose
    decorator due to the way blender registers and requires that all execute
    functions have up-front (self, context) as explicit keywords args, using
    *args and **wkargs does not pass this test.

    Always call with parentheses, like:
    @reporting.handle_operator()
    def execute():

    @reporting.handle_operator(silent=True)
    def execute():
    """

    def decorator(function: callable) -> callable:

        def wrapper(self, context):
            """Primary wrapper, decide dynamically how to execute."""
            if IS_OPTED_IN:
                _start_session()
                res = wrapper_transact(self, context)
                _end_session()
                return res
            else:
                return wrapper_non_transact(self, context)

        def wrapper_transact(self, context):
            """Wrapper with session tracking."""
            with sentry_sdk.start_transaction(op="operator",
                                              name=self.bl_idname
                                              ) as transaction:
                try:
                    # Function here is the operator execute function.
                    res = function(self, context)
                    if res == {'FINISHED'}:
                        transaction.set_status(TRANSACT_OK)
                    elif res == {'CANCELLED'}:
                        transaction.set_status(TRANSACT_CANCEL)
                except Exception as e:
                    # Intentionally broad to handle everything.
                    err = traceback.format_exc()
                    print(err)  # Always print raw traceback.
                    err = _sanitize_paths(err)
                    transaction.set_status(TRANSACT_FAIL)
                    capture_exception(e)

                    if len(err) > MAX_TAG_CHARS:
                        # Pick the last N characters of the error message.
                        err_shrt = err[-MAX_TAG_CHARS:]
                    else:
                        err_shrt = err
                    capture_message("unhandled_ops_error", err_shrt, 'fatal')

                    _set_session_crashed()

                    if silent is False:
                        bpy.ops.poliigon.report_error(
                            'INVOKE_DEFAULT', error_report=err)
                    return {'CANCELLED'}
            return res

        def wrapper_non_transact(self, context):
            """Wrapper without session tracking."""
            try:
                # Function here is the operator execute function.
                res = function(self, context)
            except Exception:
                # Intentionally broad to handle everything.
                err = traceback.format_exc()
                print(err)  # Always print raw traceback.

                if silent is False:
                    bpy.ops.poliigon.report_error(
                        'INVOKE_DEFAULT', error_report=err)
                return {'CANCELLED'}
            return res

        return wrapper
    return decorator


def handle_function(silent=True, transact=True):
    """Decorator for general purpose functions.

    Silent by default as it is often in code that is running async and would
    disrupt the user.

    Always call with parentheses, like:
    @reporting.handle_function()
    def my_function():

    @reporting.handle_function(silent=True)
    def my_function():
    """

    def decorator(function: callable) -> callable:

        def wrapper(*args, **kwargs):
            """Primary wrapper, decide dynamically how to execute."""
            if transact and IS_OPTED_IN:
                return wrapper_transact(*args, **kwargs)
            else:
                return wrapper_non_transact(*args, **kwargs)

        def wrapper_transact(*args, **kwargs):
            """The wrapper for operations that should be transactions."""
            with sentry_sdk.start_transaction(op="thread",
                                              name=function.__name__
                                              ) as transaction:
                try:
                    # Function here is the operator execute function.
                    res = function(*args, **kwargs)
                except Exception as e:
                    # Intentionally broad to handle everything.
                    err = traceback.format_exc()
                    err = _sanitize_paths(err)
                    transaction.set_status(TRANSACT_FAIL)
                    capture_exception(e)

                    if len(err) > MAX_TAG_CHARS:
                        # Pick the last N characters of the error message.
                        err_shrt = err[-MAX_TAG_CHARS:]
                    else:
                        err_shrt = err
                    capture_message("unhandled_func_error", err_shrt, 'fatal')

                    # Since no popup is surfaced, don't count as crashed.
                    # _set_session_crashed()

                    if silent is False:
                        bpy.ops.poliigon.report_error(
                            'INVOKE_DEFAULT', error_report=err)
                    return None
                return res

        def wrapper_non_transact(*args, **kwargs):
            """Non transaction wrapper."""
            try:
                # Function here is the operator execute function.
                res = function(*args, **kwargs)
            except Exception as e:
                # Intentionally broad to handle everything.
                err = traceback.format_exc()
                err = _sanitize_paths(err)
                capture_exception(e)

                if silent is False:
                    bpy.ops.poliigon.report_error(
                        'INVOKE_DEFAULT', error_report=err)
                return None
            return res

        return wrapper
    return decorator


def handle_draw():
    """Decorator for draw functions which would have high trigger rates.

    Silent by default as it is often in code that is running async and would
    disrupt the user.

    Being a draw function, there is no return value. This is a safeguard so it
    is not used to decorate operational methods.

    Always call with parentheses, like:
    @reporting.handle_draw()
    def my_draw_function(self, context):
    """

    def decorator(function: callable) -> callable:
        def wrapper(self, context):
            # Not wrapped within a transaction as it is ambient.
            try:
                # Primary draw code here.
                function(self, context)
            except Exception as e:
                # Intentionally broad to handle everything.
                err = traceback.format_exc()
                err = _sanitize_paths(err)

                print(err)  # Always print raw traceback.

                if IS_OPTED_IN and str(err) not in DRAW_ERROR_CACHE:
                    DRAW_ERROR_CACHE.append(str(err))
                    capture_exception(e)

        return wrapper
    return decorator


def user_report(code_msg: str, user_msg: str, level='fatal') -> None:
    """Send a message to sentry.io outside the context of an exception."""
    if len(code_msg) > MAX_TAG_CHARS:
        # Pick the last N characters of the error message.
        code_msg = code_msg[-MAX_TAG_CHARS:]

    # No check for `IS_OPTED_IN` as the user explicitly presses 'ok' to send.
    with sentry_sdk.push_scope() as scope:
        scope.set_extra("user_message", user_msg)
        scope.set_extra("error_snippet", code_msg)
        sentry_sdk.capture_message("user_message", level)


def capture_exception(e) -> None:
    """Captures a runtime exception object to pass forward to sentry.

    Useful to ensure the function overall continues running, while still
    capturing errors around sensitive sections such as IO operations.
    Otherwise, functions are generally covered by their corresponding handler
    decorator.
    """
    err = traceback.format_exc()
    print(err)  # Always print raw traceback.
    if not IS_OPTED_IN:
        return
    sentry_sdk.capture_exception(e)


message_cache = {}


def capture_message(message: str,
                    code_msg: str = None,
                    level: str = 'fatal',
                    max_reports: int = 10
                    ) -> None:
    """Send a message to sentry.io outside the context of an exception.

    Message is the generalized, issue-grouping name while code_msg is specific
    to this call. Valid levels are: info, error, fatal.
    """

    global message_cache

    print("Message with {} status: {}, {}".format(level, message, code_msg))
    if not IS_OPTED_IN:
        return

    if message not in message_cache:
        message_cache[message] = 0

    message_cache[message] += 1

    if max_reports > 0 and message_cache[message] > max_reports:
        return

    with sentry_sdk.push_scope() as scope:
        scope.set_extra("error_snippet", code_msg)
        sentry_sdk.capture_message(message, level)


def _sanitize_paths(msg: str):
    """Strip out long userpaths from strings and replace with short name.

    Also cuts out the second and third lines, which are wrapper calls.
    """
    nth_newline = 0
    first_newline = None
    for ind in range(len(msg)):
        if msg[ind] in ["\n", "\r"]:
            if first_newline is None:
                first_newline = ind
            nth_newline += 1
        if nth_newline == 3:
            if len(msg) > ind + 1:
                msg = msg[:first_newline] + '\n' + msg[ind + 1:]
            break

    # Normalizes backslashes for better detecting subpaths in strings,
    # which can include double escaped strings.
    msg = msg.replace(r'\\', '/').replace(r'\\\\', '/')
    try:
        return re.sub(
            # case insensitive match: File "C:/path/.." or File "/path/.."
            r'(?i)File "([a-z]:){0,1}[/\\]{1,2}.*[/\\]{1,2}',
            'File "<script_path>/',
            str(msg))
    except Exception as err:
        print(err)
        return msg


def _start_session() -> None:
    """Manually start session tracking, even if there is an existing session.

    We are not actually tracking overall application sessions, but instead use
    a session to wrap around an individual operator transaction in order
    to calculate error-free users and operations.

    Intended to only start if the user performs an action (operator).

    See source reference context manager in sessions.py:
    def auto_session_tracking

    https://docs.sentry.io/platforms/python/configuration/draining/
    """
    if not IS_OPTED_IN:
        return
    hub = sentry_sdk.Hub.current

    # start_session technically takes care of ending existing ones.
    hub.start_session(session_mode="application")


def _end_session() -> None:
    """Manually end the session on behalf of sentry for opt out or app exit.

    See source reference context manager in sessions.py:
    def auto_session_tracking
    """
    if not IS_OPTED_IN:
        return
    hub = sentry_sdk.Hub.current
    hub.end_session()


def _set_session_crashed() -> None:
    """Force the session to appear as crashed.

    Had to pull this from lower level sources of the sentry lib to assign, see
    Hub.py's start_session function for partial reference, and session.py
    for updating aspects of the session variable itself.
    """
    hub = sentry_sdk.Hub.current
    client, scope = hub._stack[-1]
    if scope._session:
        scope._session.update(status="crashed")
    else:
        # This shouldn't happen, but would occur if session was explicitly
        # ended before execution actually finished (such as from nested ops).
        # Ensure we capture crashed sessions for accurate representation.
        capture_message("session_already_ended", 'fatal')
        _start_session()
        client, scope = hub._stack[-1]
        try:
            scope._session.update(status="crashed")
        except Exception as err:
            print("Failed to get scope session with error:")
            print(err)


def _flush() -> None:
    hub = sentry_sdk.Hub.current
    hub.flush()


def register(software_name, software_version, tool_version, env):
    toolv = "P4B@" + tool_version  # Make version unique across sentry org.
    initialize_sentry(software_name, software_version, toolv, env=env)


def unregister():
    if IS_OPTED_IN:
        _flush()
