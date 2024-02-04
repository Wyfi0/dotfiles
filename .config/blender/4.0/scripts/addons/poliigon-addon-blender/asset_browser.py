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
import os
import queue
import threading
import time
from subprocess import PIPE, Popen, TimeoutExpired
from typing import Dict, List, Optional, Tuple

import bpy

from . import reporting
from .asset_browser_sync_commands import (SyncCmd,
                                          SyncAssetBrowserCmd,
                                          CMD_MARKER_START,
                                          CMD_MARKER_END)
from .toolbox import cTB, Notification
from .ui import get_model_op_details
from .modules.poliigon_core import thread_manager as tm

MAX_IDLE_TIME = 60.0
TIMEOUT_ASSET_BROWSER_CLIENT_STARTUP = 10.0
MAX_THUMB_DOWNLOAD_THREADS = 10


DEBUG_HOST = False

if bpy.app.version >= (4, 0):
    type_FileSelectEntry = bpy.types.AssetRepresentation
elif bpy.app.version >= (3, 0):
    # The exact version is probably around 2.93,
    # but as the entire asset browser connection needs 3.0 this should do
    type_FileSelectEntry = bpy.types.FileSelectEntry
else:
    type_FileSelectEntry = any


# TODO(Andreas): To be replaced by appropriate logger function
def print_debug(*args) -> None:
    if not DEBUG_HOST:
        return
    print("H:", *args)


def cmd_hello(cmd: SyncAssetBrowserCmd) -> None:
    """Handles a HELLO command."""

    print_debug("Received HELLO")
    cTB.event_hello.set()
    cTB.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.HELLO_OK))


def cmd_asset_ok(cmd: SyncAssetBrowserCmd) -> None:
    """Handles an ASSET_OK command."""

    asset_data = cmd.data
    print_debug("Received ASSET_OK: ", asset_data["name"])
    cTB.queue_ack.put(SyncAssetBrowserCmd(code=SyncCmd.CMD_DONE))
    cTB.num_jobs_ok += 1


def cmd_asset_error(cmd: SyncAssetBrowserCmd) -> None:
    """Handles an ASSET_ERROR command."""

    asset_data = cmd.data
    asset_name = asset_data["name"]
    print_debug("Received ASSET_ERROR: ", asset_name)
    cTB.queue_ack.put(SyncAssetBrowserCmd(code=SyncCmd.CMD_DONE))
    error_msg = f"Asset {asset_name} failed to process."
    reporting.capture_message("asset_browser_process_fail", error_msg, "error")
    cTB.num_jobs_error += 1


def cmd_exit_ack(cmd: SyncAssetBrowserCmd, proc: Popen) -> None:
    """Handles an EXIT_ACK command."""

    print_debug("Received EXIT_ACK: Client Blender exiting")
    try:
        _, _ = proc.communicate()
    except TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
    cTB.thd_listener = None
    cTB.listener_running = False
    cTB.thd_sender = None
    cTB.sender_running = False


def check_command(buf: str) -> Tuple[Optional[SyncAssetBrowserCmd], str]:
    """Returns a valid command, otherwise None.
    Upon detecting a corrupted command, CMD_ERROR gets sent.

    Return value:
    Tuple with two entries:
    Tuple[0]: A valid command or None
    Tuple[1]: Remaining buf after either a valid command got detected or
              an broken command got removed
    """

    if CMD_MARKER_END not in buf:
        return None, buf

    pos_delimiter = buf.find(CMD_MARKER_END, 1)
    cmd_json = buf[:pos_delimiter]
    buf = buf[pos_delimiter + len(CMD_MARKER_END):]

    if CMD_MARKER_START in cmd_json:
        pos_marker_start = cmd_json.find(CMD_MARKER_START, 1)
        cmd_json = cmd_json[pos_marker_start + len(CMD_MARKER_START):]
    else:
        cTB.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.CMD_ERROR))
        cmd_json = None
    return cmd_json, buf


def thread_listener(proc: Popen) -> None:
    """Listens to commands sent by host and checks their integrity.

    In case of error requests a command to be re-send from host via CMD_ERROR.
    Valid acks are then sorted into a queue to forward them to unblock the
    sender.
    Opposed to client side, other commands are handled directly in here
    since there are no longer running commands which could block this listener
    for long.
    """

    print_debug("thread_listener")
    cTB.listener_running = True
    buf = ""
    while cTB.listener_running:
        try:
            buf += proc.stderr.readline()
        except ValueError:
            break  # stderr handle got closed
        except KeyboardInterrupt as e:
            reporting.capture_exception(e)
            continue

        cmd_json, buf = check_command(buf)
        if cmd_json is None:
            continue

        try:
            cmd = SyncAssetBrowserCmd.from_json(cmd_json)
            if cmd.code == SyncCmd.CMD_ERROR:
                # Forward ack to thread_sender
                cTB.queue_ack.put(cmd)
            elif cmd.code == SyncCmd.ASSET_OK:
                cmd_asset_ok(cmd)
            elif cmd.code == SyncCmd.ASSET_ERROR:
                cmd_asset_error(cmd)
            elif cmd.code == SyncCmd.EXIT_ACK:
                cmd_exit_ack(cmd, proc)
            elif cmd.code == SyncCmd.HELLO:
                cmd_hello(cmd)
            elif cmd.code in [SyncCmd.HELLO_OK, SyncCmd.ASSET, SyncCmd.EXIT]:
                print_debug("Unexpected cmd:", cmd.code)
            else:
                print_debug("Unexpected cmd: UNKNOWN command", cmd.code)
        except Exception as e:
            cTB.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.CMD_ERROR))
            print("HOST CMD ERROR", cmd_json)
            print("    exc", e)

    print_debug("thread_listener EXIT")
    cTB.thd_listener = None
    cTB.listener_running = False


def start_listener(proc: Popen) -> None:
    """Starts thread_listener()"""

    cTB.thd_listener = threading.Thread(target=thread_listener,
                                        args=(proc, ),
                                        daemon=True)
    cTB.thd_listener.start()


def flush_queue_ack() -> None:
    """Removes all content from ack queue"""

    while not cTB.queue_ack.empty():
        try:
            cTB.queue_ack.get_nowait()
        except cTB.queue_ack.Empty:
            break


def flush_queue_send() -> None:
    """Removes all content from send queue"""

    reported = False
    while not cTB.queue_send.empty():
        try:
            cTB.queue_send.get_nowait()
        except cTB.queue_send.Empty:
            break
        if not reported:
            error_msg = "Stray ACK encountered"
            reporting.capture_message(
                "asset_browser_stray_ack", error_msg, "error")
            reported = True

    cTB.num_asset_browser_jobs = 0
    cTB.num_jobs_ok = 0
    cTB.num_jobs_error = 0


def t_status_bar_update(interval=0.5):
    """Timer function forces a redraw of Blender's status bar"""

    if cTB.asset_browser_quitting:
        return None

    # Forces statusbar redraw
    bpy.context.workspace.status_text_set_internal(None)

    # Force asset browser redraw
    for area in bpy.context.screen.areas:
        if area.type == "FILE_BROWSER" and area.ui_type == "ASSETS":
            area.tag_redraw()

    return interval


def refresh_asset_browser():
    area = None
    for win in bpy.context.window_manager.windows:
        for this_area in win.screen.areas:
            if this_area.type == "FILE_BROWSER" and this_area.ui_type == "ASSETS":
                area = this_area
                break

    # TODO(Andreas): Auto-refresh currently completely disabled in Blender 4.0
    #                I somehow need to learn how to do it there.
    if area is None or bpy.app.version >= (4, 0):
        notice = build_no_refresh_notification()
        cTB.register_notification(notice)
        return

    # Check if hasattr(bpy.context, "temp_override") and if not,
    # do an "old style" override (ie pre blender 3.2)
    unable_to_refresh = False
    try:
        # If we changed the type, we'll need it to redraw to get context.
        area.tag_redraw()
        if hasattr(bpy.context, "temp_override"):
            # Conditional is equal to bpy.app.version >= (3, 2, 0)
            with bpy.context.temp_override(area=area):
                # Unfortunately MUST also do another step here where we
                # set at least one library to be active.
                # Ideally, we only do this if we can detect nothing is
                # currently active.

                # Following doesn't work:
                # bpy.context.asset_library_ref = name
                # bpy.context.space_data.params.asset_library_ref = name

                # Works, if we already started with an asset browser window
                # *somewhere*, but isn't initially working (ie: exception)
                # if we are flipping from another type.
                if hasattr(area.spaces.active.params, "asset_library_reference"):
                    area.spaces.active.params.asset_library_reference = cTB.prefs.asset_browser_library_name
                elif hasattr(area.spaces.active.params, "asset_library_ref"):
                    area.spaces.active.params.asset_library_ref = cTB.prefs.asset_browser_library_name

                    # Works if the above works.
                    # will work if any non-current file lib active.
                    bpy.ops.asset.library_refresh("INVOKE_DEFAULT")
                else:
                    unable_to_refresh = True
        else:
            # See explanatory comments in temp_overide branch above
            if hasattr(area.spaces.active.params, "asset_library_reference"):
                area.spaces.active.params.asset_library_reference = cTB.prefs.asset_browser_library_name
            elif hasattr(area.spaces.active.params, "asset_library_ref"):
                area.spaces.active.params.asset_library_ref = cTB.prefs.asset_browser_library_name
                bpy.ops.asset.library_refresh({"area": area}, "INVOKE_DEFAULT")
            else:
                unable_to_refresh = True
    except Exception as e:  # deliberately catch any exception
        reporting.capture_exception(e)
        unable_to_refresh = True

    if unable_to_refresh:
        notice = build_no_refresh_notification()
        cTB.register_notification(notice)


def t_after_processing_done():
    """One-shot timer function doing final steps after processing finished."""

    if cTB.asset_browser_quitting:
        return None

    if bpy.app.timers.is_registered(t_status_bar_update):
        bpy.app.timers.unregister(t_status_bar_update)

    cTB.refresh_ui()

    # Force statusbar redraw
    bpy.context.workspace.status_text_set_internal(None)

    if cTB.num_asset_browser_jobs > 0:
        refresh_asset_browser()

    # Force asset browser+preferences redraw
    for area in bpy.context.screen.areas:
        if area.type == "FILE_BROWSER" and area.ui_type == "ASSETS":
            area.tag_redraw()
        elif area.type == "PREFERENCES" and bpy.context.preferences.active_section == "ADDONS":
            # TODO(Andreas): Can we somehow find out if P4B page is shown?
            area.tag_redraw()

    cTB.num_asset_browser_jobs = 0
    cTB.num_jobs_ok = 0
    cTB.num_jobs_error = 0

    return None  # one-shot, auto-unregister


def after_processing_done() -> None:
    """Final steps after asset processing finished."""

    DONE_SECONDS = 5.0

    print_debug("LIB UPDATE")
    # Make sure, the asset browser blend files get picked up,
    # so quick menu shows correct options
    cTB.f_GetLocalAssets(force=True)

    # Final actions in main thread
    bpy.app.timers.register(t_after_processing_done,
                            first_interval=DONE_SECONDS,
                            persistent=True)


def build_lost_client_notification():
    msg = ("Asset Browser synchronization client exited unexpectedly.")
    notice = Notification(
        notification_id="ASSET_BROWSER_LOST_CLIENT",
        title=msg,
        action=Notification.ActionType.POPUP_MESSAGE,
        tooltip=msg,
        allow_dismiss=True,
        ac_popup_message_body=msg,
        ac_popup_message_alert=True
    )
    return notice


def build_new_catalogue_notification():
    title = "Reload file to update asset browser catalogues."
    msg = ("We created a new catalogue file for Poliigon Library in Asset Browser. "
           "Unfortunately Blender will pick up the new catalogues only after either "
           "reloading the current blend file or restarting Blender.")
    notice = Notification(
        notification_id="ASSET_BROWSER_NEW_CATALOGUE",
        title=title,
        action=Notification.ActionType.POPUP_MESSAGE,
        tooltip=msg,
        allow_dismiss=True,
        ac_popup_message_body=msg,
        ac_popup_message_alert=False
    )
    return notice


def build_no_refresh_notification():
    title = "Sync done, press refresh in asset browser."
    msg = ("Failed to refresh the Poliigon Library in Asset Browser. "
           "Either press the Refresh Library button in Asset Browser or consider "
           "a restart of Blender.")
    notice = Notification(
        notification_id="ASSET_BROWSER_FAILED_REFRESH",
        title=title,
        action=Notification.ActionType.POPUP_MESSAGE,
        tooltip=msg,
        allow_dismiss=True,
        ac_popup_message_body=msg,
        ac_popup_message_alert=False
    )
    return notice


def check_sender_continue() -> bool:
    """Checks if thread_sender is supposed to continuie."""

    if not cTB.sender_running:
        return False  # normal exit condition
    if cTB.proc_blender_client is None:
        print_debug("Client process is None!")
        error_msg = "Client process is None"
        reporting.capture_message(
            "asset_browser_client_none", error_msg, "error")
        return False
    exit_code_client = cTB.proc_blender_client.poll()
    if exit_code_client is not None:
        print_debug("Client exited unexpectedly", exit_code_client)
        notice = build_lost_client_notification()
        cTB.register_notification(notice)

        error_msg = f"Client exited unexpectedly ({exit_code_client})"
        reporting.capture_message(
            "asset_browser_client_exit", error_msg, "error")

        # Close client's "back channel" to unblock thread_listener
        cTB.listener_running = False
        cTB.proc_blender_client.stderr.close()
        return False
    return True


def thread_sender() -> None:
    """Sends commands to client.

    For commands expecting an acknowledge message the thread will then block
    until the ack is received (or possibly resend the command if CMD_ERROR is
    received).
    """

    TIMEOUT_QUEUE = 1.0  # Used to poll forr flags while waiting
    NUM_ACK_RETRIES = 60  # NUM_ACK_RETRIES * TIMEOUT_QUEUE = time to ack timeout

    print_debug("thread_sender")
    cTB.sender_running = True
    time_remaining = MAX_IDLE_TIME

    while cTB.sender_running:
        # Get rid of any unwanted acks from previous commands
        flush_queue_ack()

        if cTB.asset_browser_jobs_cancelled:
            flush_queue_send()
            after_processing_done()
            cTB.asset_browser_jobs_cancelled = False

        # Wait for something to send
        try:
            cmd_send = cTB.queue_send.get(timeout=TIMEOUT_QUEUE)
            cTB.queue_send.task_done()
            time_remaining = MAX_IDLE_TIME
        except queue.Empty:
            if cTB.sender_running:
                time_remaining -= 1.0
                if time_remaining > 0.0:
                    continue
                else:
                    print_debug("Idle time reached")
                    SyncAssetBrowserCmd(code=SyncCmd.EXIT).send_to_process(
                        cTB.proc_blender_client)
                    cTB.sender_running = False

        if not check_sender_continue():
            break

        print_debug("Send:", cmd_send.code)
        cmd_send.send_to_process(cTB.proc_blender_client)

        # Depending on sent command code, we are already done
        if cmd_send.code in [SyncCmd.HELLO_OK, SyncCmd.STILL_THERE]:
            # HELLO_OK and STILL_THERE are fire and forget
            continue

        # Wait for acknowledge message
        retries = NUM_ACK_RETRIES
        while retries > 0 and cTB.sender_running:
            try:
                cmd_ack = cTB.queue_ack.get(timeout=TIMEOUT_QUEUE)
                cTB.queue_ack.task_done()
            except queue.Empty:
                cmd_ack = None
                if not check_sender_continue():
                    break

            retries -= 1
            if cmd_ack is None:
                # queue timeout, unless retries are exhausted continue to wait
                if retries == 0:
                    cTB.asset_browser_jobs_cancelled = True
                    error_msg = "ACK timeout"
                    reporting.capture_message(
                        "asset_browser_ack_timeout", error_msg, "error")
            elif cmd_ack.code == SyncCmd.CMD_ERROR:
                # last sent command was not received well -> resend
                if retries > 0:
                    cmd_send.send_to_process(cTB.proc_blender_client)
                else:
                    cTB.asset_browser_jobs_cancelled = True
                    error_msg = "ACK error, no more retries"
                    reporting.capture_message(
                        "asset_browser_retry_max", error_msg, "error")
            elif cmd_ack.code == SyncCmd.CMD_DONE:
                # last sent command was ok, continue with next
                break

        cTB.refresh_ui()

        # Finally if there are no more commands in queue
        # refresh Blender's Asset Browser library
        if cTB.queue_send.empty():
            after_processing_done()

    print_debug("thread_sender EXIT")
    cTB.num_asset_browser_jobs = 0
    cTB.num_jobs_ok = 0
    cTB.num_jobs_error = 0
    cTB.thd_sender = None
    cTB.sender_running = False


def start_sender(proc: Popen) -> None:
    """Starts thread_sender()."""

    cTB.thd_sender = threading.Thread(target=thread_sender,
                                      # args=(proc, ),
                                      daemon=True)
    cTB.thd_sender.start()


def get_poliigon_library():
    """Returns Poliigons's Asset Browser library, if any."""

    lib_poliigon = None
    addon_lib_path = os.path.normpath(cTB.vSettings["library"])
    for lib in bpy.context.preferences.filepaths.asset_libraries:
        if os.path.normpath(lib.path) == os.path.normpath(addon_lib_path):
            # Not checking name here, user may have manually renamed library
            lib_poliigon = lib
            break
        elif lib.name == cTB.prefs.asset_browser_library_name:
            lib_poliigon = None
            # TODO(Andreas): check with Patrick
            # So, here we have a library with correct name, but wrong path.
            # Either user created one manually, in which case I'm not sure,
            # we should touch the path. Or user changed the primary P4B library
            # directrory, in which case we would actually need to change the path?
            # A conundrum...
            break
    return lib_poliigon


def check_library_name_exists(library_name: str) -> bool:
    for lib in bpy.context.preferences.filepaths.asset_libraries:
        if lib.name == library_name:
            return True
    return False


def create_poliigon_library(force: bool = False):
    """Creates a´new library in Blender's Asset Browser,
    if not already done so before.
    """
    lib_poliigon = get_poliigon_library()
    if lib_poliigon is not None:
        if force:
            lib_poliigon.path = cTB.vSettings["library"]
        return lib_poliigon

    if cTB.vSettings["library"] == "":
        return None

    # Create new library
    libraries_before = list(bpy.context.preferences.filepaths.asset_libraries)

    # From: https://blender.stackexchange.com/questions/267676/create-a-new-asset-library-and-get-it-in-a-variable
    result = bpy.ops.preferences.asset_library_add(
        directory=cTB.vSettings["library"])
    if result == {"CANCELLED"}:
        error_msg = "Operator asset_library_add failed!"
        reporting.capture_message(
            "asset_browser_lib_create", error_msg, "error")
        return None

    libraries_after = list(bpy.context.preferences.filepaths.asset_libraries)
    libraries_new = [
        lib for lib in libraries_after if lib not in libraries_before]
    if len(libraries_new) == 0:
        error_msg = "Failed to find freshly created library!"
        reporting.capture_message(
            "asset_browser_lib_create_find", error_msg, "error")
        return None

    lib_poliigon = libraries_new[0]
    if os.path.normpath(lib_poliigon.path) == os.path.normpath(cTB.vSettings["library"]):
        library_name = cTB.prefs.asset_browser_library_name
        count = 1
        while check_library_name_exists(library_name):
            library_name = f"{cTB.prefs.asset_browser_library_name}.{count:03}"
            count += 1
        lib_poliigon.name = library_name

    path_cat = os.path.join(cTB.vSettings["library"],
                            "blender_assets.cats.txt")
    if not os.path.exists(path_cat):
        notice = build_new_catalogue_notification()
        cTB.register_notification(notice)

    return lib_poliigon


def start_blender_client() -> bool:
    """Starts a Blender subprocess."""

    if cTB.proc_blender_client is not None:
        print_debug("Blender client still running")
        error_msg = "Process not None, when starting fresh!"
        reporting.capture_message(
            "asset_browser_client_running", error_msg, "error")

    print_debug("Starting Blender client...")
    cwd = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    path_client_script = os.path.join(cwd, "asset_browser_sync_client.py")
    if not os.path.isfile(path_client_script):
        cTB.proc_blender_client = None
        error_msg = "Client script missing!"
        reporting.capture_message(
            "asset_browser_script_missing", error_msg, "error")
        return False

    cmd_blender = [bpy.app.binary_path]
    cmd_blender.append("--background")
    cmd_blender.append("--factory-startup")
    cmd_blender.append("--python")
    cmd_blender.append(path_client_script)
    cmd_blender.append("--")  # Blender ignores all following command line args
    path_cat = os.path.join(cTB.vSettings["library"],
                            "blender_assets.cats.txt")
    cmd_blender.append("--poliigon_cat_file")
    cmd_blender.append(path_cat)
    path_categories = os.path.join(cTB.gSettingsDir, "TB_Categories.json")
    cmd_blender.append("--poliigon_categories")
    cmd_blender.append(path_categories)

    cTB.proc_blender_client = Popen(cmd_blender,
                                    cwd=cwd,
                                    stdin=PIPE,
                                    stderr=PIPE,
                                    text=True)
    return True


def wait_for_client(timeout: float = None) -> bool:
    """Waits for hello event, which gets set upon receiving a HELLO message."""

    print_debug("Waiting for Blender client...")
    event_set = cTB.event_hello.wait(timeout)
    if event_set:
        cTB.event_hello.clear()
    return event_set


def get_blender_process() -> bool:
    """Checks if the Blender client is already running and
    starts one (including all other infrastructure) if needed.
    """

    if cTB.proc_blender_client is not None:
        old_exit_code = cTB.proc_blender_client.poll()
    if cTB.proc_blender_client is None or old_exit_code is not None:
        if cTB.thd_listener is not None:
            print_debug("listener still running")
            error_msg = "get_blender_process(): listener still running!"
            reporting.capture_message(
                "asset_browser_listener_running", error_msg, "error")
        if cTB.thd_sender is not None:
            print_debug("sender still running")
            error_msg = "get_blender_process(): sender still running!"
            reporting.capture_message(
                "asset_browser_process_running", error_msg, "error")

        if not start_blender_client():
            return False

        cTB.queue_send = queue.Queue()
        cTB.queue_ack = queue.Queue()
        cTB.event_hello = threading.Event()

        start_listener(cTB.proc_blender_client)
        start_sender(cTB.proc_blender_client)
    else:
        cTB.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.STILL_THERE))

    return True


def thread_prepare_local_assets(asset_name: str):
    thread_manager = tm.ThreadManager()

    cTB.f_GetLocalAssetsThread()

    if asset_name == "":
        assets_local = []
        for asset_type in cTB.vAssets["local"]:
            if asset_type == "Brushes":
                continue  # Not supported by Blender's asset browser

            for asset_data_local in cTB.vAssets["local"][asset_type].values():
                asset_name = asset_data_local["name"]

                if asset_name not in cTB.vAssets["my_assets"][asset_type]:
                    continue  # Asset not purchased

                assets_local.append(asset_name)
    else:
        assets_local = [asset_name]

    did_search = False
    assets_need_thumb = []
    futures_get_assets = []
    for _asset_name in assets_local:
        path_thumb = cTB.f_GetThumbnailPath(_asset_name, 0)
        if not os.path.exists(path_thumb):
            fut = thread_manager.queue_thread(cTB.f_APIGetAssets,
                                              tm.PoolKeys.PREVIEW_DL,
                                              MAX_THUMB_DOWNLOAD_THREADS,
                                              False,
                                              "poliigon",
                                              1,
                                              1000,
                                              _asset_name,
                                              f"poliigon@All Assets@{_asset_name}",
                                              vBackground=True,
                                              vTime=time.time())
            futures_get_assets.append(fut)
            assets_need_thumb.append((_asset_name, path_thumb))
            did_search = True

    for fut in futures_get_assets:
        fut.result()

    if did_search:
        cTB.f_GetLocalAssetsThread()

    futures_download = []
    for _asset_name, path_thumb in assets_need_thumb:
        fut = thread_manager.queue_thread(cTB.f_DownloadPreview,
                                          tm.PoolKeys.PREVIEW_DL,
                                          MAX_THUMB_DOWNLOAD_THREADS,
                                          False,
                                          _asset_name,
                                          0)
        futures_download.append(fut)

    for fut in futures_download:
        fut.result()

    for _asset_name, path_thumb in assets_need_thumb:
        if os.path.exists(path_thumb):
            continue
        # In this case we must rely on automatic thumb generation on client
        error_msg = f"send_asset_data(): Thumbnail failed to download for {_asset_name}!"
        reporting.capture_message("asset_browser_thumb_dl", error_msg, "error")


def thread_start_sync_local_client():
    if not get_blender_process():
        print_debug("Failed to get Blender client process!")
        error_msg = "Failed to get Blender client process."
        reporting.capture_message(
            "asset_browser_fail_client_p", error_msg, "error")
        return

    if not wait_for_client(timeout=TIMEOUT_ASSET_BROWSER_CLIENT_STARTUP):
        print_debug("Blender client failed to say hello!")
        error_msg = "No Hello message from Blender client."
        reporting.capture_message("asset_browser_no_hello", error_msg, "error")
        return


def thread_initiate_asset_synchronization(asset_name: str, force: bool):
    with cTB.lock_client_start:
        thd_start_client = threading.Thread(
            target=thread_start_sync_local_client)
        thd_start_client.start()

        thd_prepare_assets = threading.Thread(
            target=thread_prepare_local_assets, args=(asset_name, ))
        thd_prepare_assets.start()

        thd_start_client.join()
        thd_prepare_assets.join()

        if cTB.proc_blender_client is None:
            return

    if asset_name == "":
        send_all_local_assets(force)
    else:
        if not send_single_asset(asset_name, force):
            error_msg = f"send_single_asset() failed unexpectedly: {asset_name}."
            reporting.capture_message(
                "asset_browser_single_asset", error_msg, "error")


def get_asset_job_parameters(asset_data: Dict,
                             asset_data_local: Dict
                             ) -> Optional[Dict]:
    """Provides size and (if needed) lod for Asset Browser generation."""

    asset_name = asset_data["name"]
    asset_type = asset_data["type"]
    sizes = asset_data["sizes"]
    sizes_local = asset_data_local["sizes"]

    check_sizes = sizes_local if len(sizes_local) > 0 else sizes
    if len(check_sizes) == 0:
        error_msg = f"get_asset_job_parameters(): No sizes for asset {asset_name}!"
        reporting.capture_message("asset_browser_no_sizes", error_msg, "error")
        return None

    params = {}

    if asset_type == "Textures":
        params["size"] = cTB.f_GetClosestSize(
            check_sizes, cTB.vSettings["res"])
        params["size_bg"] = None
        params["lod"] = None
    elif asset_type == "Models":
        params["size"] = cTB.f_GetClosestSize(
            check_sizes, cTB.vSettings["mres"])
        params["size_bg"] = None
        lod, _, _ = get_model_op_details(
            asset_name, asset_type, params["size"])
        params["lod"] = lod if len(lod) > 0 else "NONE"
    elif asset_type == "HDRIs":
        size = cTB.f_GetClosestSize(check_sizes, cTB.vSettings["hdri"])
        params["size"] = size
        if cTB.vSettings["hdri_use_jpg_bg"]:
            params["size_bg"] = f"{cTB.vSettings['hdrib']}_JPG"
        else:
            params["size_bg"] = f"{size}_EXR"
        params["lod"] = None
    elif asset_type == "Brushes":
        params["size"] = cTB.f_GetClosestSize(
            check_sizes, cTB.vSettings["brush"])
        params["size_bg"] = None
        params["lod"] = None

    params["thumb"] = cTB.f_GetThumbnailPath(asset_name, 0)
    return params


def send_asset_data(asset_data_local: Dict,
                    asset_name: str,
                    asset_type: str,
                    force: bool) -> bool:
    """Queues a single ASSET job to be send to the client."""

    asset_data = cTB.vAssets["my_assets"][asset_type][asset_name]
    params = get_asset_job_parameters(asset_data, asset_data_local)
    if params is None:
        return False

    if not os.path.exists(params["thumb"]):
        # In this case we must rely on automatic thumb generation on client
        error_msg = f"send_asset_data(): No thumbnail for {asset_name}!"
        reporting.capture_message("asset_browser_no_thumb", error_msg, "error")

    # Need copy as we are updating the dict
    asset_data = asset_data.copy()
    asset_data.update(asset_data_local)

    path_asset_file = asset_data["files"][0]
    directory = os.path.dirname(path_asset_file)
    filename = f"{asset_name}_LIB.blend"
    params["path_result"] = os.path.join(directory, filename)

    if not force and os.path.exists(params["path_result"]):
        print_debug("Skipping already existing:", asset_name)
        return False

    if asset_type == "Brushes":
        pass  # Brushes not supported, yet
    elif asset_type == "HDRIs":
        pass  # nothing to do here
    elif asset_type == "Models":
        params["lod"] = "NONE"
    elif asset_type == "Textures":
        pass  # nothing to do here

    # Register timer to get status bar redrawn in regular intervals
    if not bpy.app.timers.is_registered(t_status_bar_update):
        bpy.app.timers.register(
            t_status_bar_update, first_interval=0.5, persistent=True)

    cTB.num_asset_browser_jobs += 1
    cTB.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.ASSET,
                                           data=asset_data,
                                           params=params))
    return True


def send_all_local_assets(force: bool) -> int:
    """Queues all local assets, which have not been processed before.

    Arguments:
    force: Set to True to process _all_ local assets. Even if processed before.
    """

    num_assets = 0
    for asset_type in cTB.vAssets["local"]:
        if asset_type == "Brushes":
            # Not supported by Blender's asset browser
            continue

        for asset_data_local in cTB.vAssets["local"][asset_type].values():
            asset_name = asset_data_local["name"]

            if asset_name not in cTB.vAssets["my_assets"][asset_type]:
                print_debug("Skipping: Unpurchased local asset:", asset_name)
                continue

            result_ok = send_asset_data(
                asset_data_local, asset_name, asset_type, force)
            if result_ok:
                num_assets += 1

    cTB.refresh_ui()
    return num_assets


def send_single_asset(asset_name: str, force: bool) -> bool:
    """Queues one local asset, which has not been processed before.

    Arguments:
    force: Set to True to process the local assets, even if processed before.
    """

    found = False
    for asset_type in cTB.vAssets["local"]:
        if asset_type == "Brushes":
            # Not supported by Blender's asset browser
            continue

        for asset_data_local in cTB.vAssets["local"][asset_type].values():
            if asset_data_local["name"] == asset_name:
                found = True
                break
        if found:
            break

    if not found:
        error_msg = f"send_single_asset(): Asset {asset_name} not found!"
        reporting.capture_message("asset_browser_no_asset", error_msg, "error")
        return False

    result = send_asset_data(asset_data_local, asset_name, asset_type, force)

    cTB.refresh_ui()
    return result


# Following functions are needed by Asset Browser operators and panels
def get_asset_name_from_browser_asset(asset_file: type_FileSelectEntry
                                      ) -> str:
    """Returns Poliigon's asset name for an asset file in the Asset Browser"""

    path_asset = asset_file.relative_path
    pos = path_asset.find("_LIB.blend", 1)
    path_asset = path_asset[:pos]
    return os.path.basename(path_asset)


def get_asset_data_from_browser_asset(asset_file: type_FileSelectEntry,
                                      asset_name: str
                                      ) -> Dict:
    """Returns asset data dict for an asset file in Blender's Asset Browser"""

    asset_data = None
    asset_type = None

    # Try to get asset type from tags
    for tag in asset_file.asset_data.tags:
        if tag.name in cTB.vAssetTypes:
            asset_type = tag.name

    if asset_type is not None:
        asset_data = cTB.vAssets["my_assets"][asset_type].get(asset_name, None)

        if asset_data is None:
            print("ASSET NOT FOUND")
            # TODO(Andreas): user notification
            return None
    else:
        # Maybe someone messed with out tags, search local assets
        for asset_type in cTB.vAssets["my_assets"]:
            if asset_name not in cTB.vAssets["my_assets"][asset_type]:
                continue
            asset_data = cTB.vAssets["my_assets"][asset_type][asset_name]
            break

    return asset_data


def is_asset_browser(context) -> bool:
    """Returns true, if the area is an Asset Browser."""

    is_file_browser = context.area.type == "FILE_BROWSER"
    _is_asset_browser = context.area.ui_type == "ASSETS"
    if not (is_file_browser and _is_asset_browser):
        return False

    if bpy.app.version > (4, 0):
        has_asset_browser_ref = hasattr(context, "asset_library_reference")
    else:
        has_asset_browser_ref = hasattr(context.space_data.params, "asset_library_ref")
    if not has_asset_browser_ref:
        return False

    return True


def is_poliigon_library(context, incl_all_libs: bool = True) -> bool:
    """Returns True, if the active library is a Poliigon library."""

    if bpy.app.version >= (4, 0):
        try:
            library_name = context.area.spaces.active.params.asset_library_reference
        except Exception as e:
            reporting.capture_exception(e)
            return True
    else:
        library_name = context.space_data.params.asset_library_ref

    # TODO(Andreas): Use context.area.spaces.active.params.asset_library_ref instead?
    is_poliigon_lib = library_name == cTB.prefs.asset_browser_library_name
    is_all_libs = library_name == "ALL"
    return is_poliigon_lib or (incl_all_libs and is_all_libs)


def is_only_poliigon_selected(context) -> bool:
    """Returns True, if only Poliigon assets are selected in Asset Browser."""

    asset_files = get_selected_assets(context)
    is_poliigon_only = True
    for _asset_file in asset_files:
        asset_name = get_asset_name_from_browser_asset(_asset_file)
        asset_data = get_asset_data_from_browser_asset(_asset_file, asset_name)
        if asset_data is None:
            is_poliigon_only = False
            break
    return is_poliigon_only


def check_handles_and_selected_files(context) -> Tuple:
    selected_asset_files = None
    asset_file_handle = None
    if bpy.app.version >= (4, 0):
        has_selected_assets = hasattr(context, "selected_assets")
        if has_selected_assets:
            selected_asset_files = context.selected_assets
        has_asset_handle = hasattr(context, "asset")
        if has_asset_handle:
            asset_file_handle = context.asset
    else:
        has_selected_assets = hasattr(context, "selected_asset_files")
        if has_selected_assets:
            selected_asset_files = context.selected_asset_files
        has_asset_handle = hasattr(context, "asset_file_handle")
        if has_asset_handle:
            asset_file_handle = context.asset_file_handle
    return selected_asset_files, asset_file_handle


def get_selected_assets(context) -> List:
    selected_asset_files, asset_file_handle = check_handles_and_selected_files(
        context)
    if selected_asset_files is not None and len(selected_asset_files) > 0:
        return selected_asset_files
    elif asset_file_handle is not None:
        return [asset_file_handle]
    else:
        return []


def get_num_selected_assets(context) -> int:
    selected_assets = get_selected_assets(context)
    return len(selected_assets)
