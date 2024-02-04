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

"""Standalone blender startup script used to generate asset blend files.

General Asset Browser links:
Asset Catalogs: https://wiki.blender.org/wiki/Source/Architecture/Asset_System/Catalogs
Asset Operators: https://docs.blender.org/api/current/bpy.ops.asset.html
AssetMetaData: https://docs.blender.org/api/current/bpy.types.AssetMetaData.html#bpy.types.AssetMetaData
Catalogs and save: https://blender.stackexchange.com/questions/284833/get-asset-browser-catalogs-in-case-of-unsaved-changes

Operator overriding:
https://blender.stackexchange.com/questions/248274/a-comprehensive-list-of-operator-overrides
https://blender.stackexchange.com/questions/129989/override-context-for-operator-called-from-panel
https://blender.stackexchange.com/questions/182713/how-to-use-context-override-on-the-disable-and-keep-transform-operator
https://blender.stackexchange.com/questions/273474/how-to-override-context-to-launch-ops-commands-in-text-editor-3-2
https://blender.stackexchange.com/questions/875/proper-bpy-ops-context-setup-in-a-plugin

Asset browser related, not much use in here:
https://blender.stackexchange.com/questions/262284/how-do-i-access-the-list-of-selected-assets-from-an-event-in-python
https://blender.stackexchange.com/questions/261213/get-the-source-path-of-the-assets-in-asset-browser-using-python
"""

import argparse
import hashlib
import json
import os
import queue
import shutil
import sys
import time
import threading
import uuid

import bpy

from dataclasses import asdict, dataclass
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple
sys.path.insert(0, os.path.dirname(__file__))
from asset_browser_sync_commands import (SyncCmd,
                                         SyncAssetBrowserCmd,
                                         CMD_MARKER_START,
                                         CMD_MARKER_END)


DEBUG_CLIENT = False


def print_debug(*args, file=sys.stdout) -> None:
    """Use for printing in client script"""

    if not DEBUG_CLIENT:
        return
    print("          C:", *args, file=file)


@dataclass
class ScriptContext():
    path_cat: Optional[str] = None  # from command line args
    path_categories: Optional[str] = None  # from command line args

    poliigon_categories: Optional[Dict] = None

    listener_running: bool = False
    thd_listener: Optional[threading.Thread] = None

    sender_running: bool = False
    thd_sender: Optional[threading.Thread] = None

    queue_cmd: Optional[queue.Queue] = None
    queue_send: Optional[queue.Queue] = None
    queue_ack: Optional[queue.Queue] = None

    main_running: bool = False


def command_line_args(ctx: ScriptContext) -> bool:
    """Parses command line args and stores parameters in context."""

    ctx.path_cat = None
    ctx.path_categories = None

    # Skip Blender's own command line args
    argv = sys.argv
    try:
        idx_arg = argv.index("--") + 1
    except ValueError:
        idx_arg = None
    if idx_arg is None or idx_arg >= len(argv):
        return

    argv = argv[idx_arg:]

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-pcf", "--poliigon_cat_file",
                            help="Path to catalog file",
                            required=True)
        parser.add_argument("-pc", "--poliigon_categories",
                            help="Path to file with Poliigon categories",
                            required=True)
        args = parser.parse_args(argv)
    except Exception as e:
        print_debug(e)

    if not args.poliigon_cat_file:
        print_debug("Lacking path to Blender cat file in commandline arguments!")
        ctx.path_cat = None
        return False
    else:
        ctx.path_cat = args.poliigon_cat_file

    if not args.poliigon_categories:
        print_debug("Lacking path to Poliigon categories file in commandline arguments!")
        ctx.path_categories = None
        return False
    else:
        ctx.path_categories = args.poliigon_categories

    return True


def wait_for_p4b_local_assets():
    """Makes sure, the local assets dict is populated."""

    bpy.ops.poliigon.get_local_asset_sync()


def check_command(ctx: ScriptContext,
                  buf: str) -> Tuple[Optional[SyncAssetBrowserCmd], str]:
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
        ctx.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.CMD_ERROR))
        cmd_json = None
    return cmd_json, buf


def thread_listener(ctx: ScriptContext) -> None:
    """Listens to commands sent by host and checks their integrity.

    In case of error requests a command to be re-send from host via CMD_ERROR.
    Valid commands are then sorted into two queues, one for received acks
    (forwarding them to unblock sender), one for job commands (forwarding
    them to main loop).
    """

    print_debug("thread_listener")
    ctx.listener_running = True
    buf = ""
    while ctx.listener_running:
        # Wait for messages from host, concatenating received lines into buf
        try:
            buf += sys.stdin.readline()
        except KeyboardInterrupt:
            time.sleep(0.5)
            if ctx.listener_running:
                continue

        if not ctx.listener_running:
            break

        cmd_json, buf = check_command(ctx, buf)
        if cmd_json is None:
            continue

        try:
            cmd_from_host = SyncAssetBrowserCmd.from_json(cmd_json)
            if cmd_from_host.code == SyncCmd.CMD_ERROR:
                # Forward ack to thread_sender
                ctx.queue_ack.put(cmd_from_host)
            else:
                # Forward job command to main loop
                ctx.queue_cmd.put(cmd_from_host)
        except Exception as e:
            ctx.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.CMD_ERROR))
            print_debug("CMD ERROR", cmd_json)
            print_debug("    exc", e)

    print_debug("thread_listener EXIT")
    ctx.thd_listener = None


def start_listener(ctx: ScriptContext) -> None:
    """Starts thread_listener()"""

    ctx.thd_listener = threading.Thread(target=thread_listener,
                                        args=(ctx, ),
                                        daemon=True)
    ctx.thd_listener.start()


def flush_queue_ack(ctx: ScriptContext) -> None:
    """Removes all content from ack queue"""

    while not ctx.queue_ack.empty():
        try:
            ctx.queue_ack.get_nowait()
        except ctx.queue_ack.Empty:
            break


def shutdown_on_error(ctx: ScriptContext) -> None:
    """Shuts the client down"""

    # The client quits and host will pick up the "client loss" due to timeouts
    ctx.sender_running = False
    ctx.listener_running = False
    ctx.main_running = False
    sys.stdin.close()  # unblock listener


def thread_sender(ctx: ScriptContext) -> None:
    """Sends commands to host.

    For commands expecting an acknowledge message the thread will then block
    until the ack is received (or possibly resend the command if CMD_ERROR is
    received).
    """

    print_debug("thread_sender")
    ctx.sender_running = True
    while ctx.sender_running:
        # Get rid of any unwanted acks from previous commands
        flush_queue_ack(ctx)

        # Wait for something to send
        try:
            cmd_send = ctx.queue_send.get(timeout=1.0)
            ctx.queue_send.task_done()
        except queue.Empty:
            if ctx.sender_running:
                continue

        if not ctx.sender_running:
            break

        print_debug("Send:", cmd_send.code)
        cmd_send.send_to_stdio()

        # Depending on sent command code, we are already done
        if cmd_send.code in [SyncCmd.ASSET_OK, SyncCmd.ASSET_ERROR]:
            # ASSET_OK, ASSET_ERROR are fire and forget,
            # just proceed wih next command
            continue
        elif cmd_send.code == SyncCmd.EXIT_ACK:
            # EXIT_ACK is fire and forget, we are done here
            ctx.sender_running = False
            break

        # Wait for acknowledge message
        retries = 3
        while retries > 0 and ctx.sender_running:
            try:
                cmd_ack = ctx.queue_ack.get(timeout=15.0)
                ctx.queue_ack.task_done()
            except queue.Empty:
                cmd_ack = None

            retries -= 1
            if cmd_ack is None:
                # queue timeout, unless retries are exhausted continue to wait
                if retries == 0:
                    # Unlikely we can gracefully recover
                    shutdown_on_error(ctx)
                    break
            elif cmd_ack.code == SyncCmd.CMD_ERROR:
                # last sent command was not received well -> resend
                if retries > 0:
                    cmd_send.send_to_stdio()
                else:
                    # Unlikely we can gracefully recover
                    shutdown_on_error(ctx)
            elif cmd_ack.code == SyncCmd.CMD_DONE:
                # last sent command was ok, continue with next
                break

    print_debug("thread_sender EXIT")
    ctx.thd_sender = None


def start_sender(ctx: ScriptContext) -> None:
    """Starts thread_sender()"""

    ctx.thd_sender = threading.Thread(target=thread_sender,
                                      args=(ctx, ),
                                      daemon=True)
    ctx.thd_sender.start()


def startup(ctx: ScriptContext) -> None:
    bpy.ops.preferences.addon_enable(module="poliigon-addon-blender")

    wait_for_p4b_local_assets()

    if not read_poliigon_categories(ctx):
        return False

    ctx.queue_cmd = queue.Queue()
    ctx.queue_send = queue.Queue()
    ctx.queue_ack = queue.Queue()

    start_listener(ctx)
    start_sender(ctx)

    ctx.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.HELLO))
    return True


def reset_blend():
    """Prepares a fresh blend file for stuff to be imported into."""

    bpy.ops.wm.read_homefile(use_empty=True)

    # To be safe deselect all
    for obj in bpy.data.objects:
        obj.select_set(False)


def save_blend(path: str) -> bool:
    """Saves the current blend file."""

    # Remove previous file
    # (host will re-process assets only, if force parameter was set)
    if os.path.exists(path):
        os.remove(path)

    result = bpy.ops.wm.save_mainfile(filepath=path,
                                      check_existing=False,
                                      exit=False)
    return result == {"FINISHED"}


# Based on code from:
# https://blender.stackexchange.com/questions/249316/python-set-asset-library-tags-and-catalogs
def get_catalog_dict(ctx: ScriptContext) -> Dict:
    """Reads blender's catalogue and returns a dictionary with its content.

    Return value:
    Dict: {catalog tree path: (uuid, catalog tree path, catalog name)}
    """

    if not os.path.exists(ctx.path_cat):
        return {}
    catalogs = {}
    with open(ctx.path_cat, "r") as file_catalogs:
        for line in file_catalogs.readlines():
            if line.startswith(("#", "VERSION", "\n")):
                continue
            # Each line contains : 'uuid:catalog_tree:catalog_name' + eol ('\n')
            uuid, tree_path, name = line.split(":")
            name = name.split("\n")[0]
            catalogs[tree_path] = (uuid, tree_path, name)
    return catalogs


def catalog_file_header(version: int = 1):
    """Returns the standard header of a catalog file."""

    header = ("# This is an Asset Catalog Definition file for Blender.\n"
              "#\n"
              "# Empty lines and lines starting with `#` will be ignored.\n"
              "# The first non-ignored line should be the version indicator.\n"
              '# Other lines are of the format "UUID:catalog/path/for/assets:simple catalog name"\n'
              "\n"
              f"VERSION {version}\n"
              "\n")
    return header


def write_catalog_file(ctx: ScriptContext, catalog_dict: Dict) -> bool:
    """Writes a catalog dict into a new catalog file,
    replacing the old file upon success.
    """

    path_cat_temp = ctx.path_cat + ".TEMP"
    path_cat_bak = ctx.path_cat + ".BAK"
    try:
        # Write into temporary file
        with open(path_cat_temp, "w") as file_catalogs:
            header = catalog_file_header()
            file_catalogs.write(header)
            for uuid, tree_path, name in catalog_dict.values():
                file_catalogs.write(f"{uuid}:{tree_path}:{name}\n")

        # Replace existing catalog file (if any) with above temporary file
        if os.path.exists(ctx.path_cat):
            shutil.move(ctx.path_cat, path_cat_bak)
        shutil.move(path_cat_temp, ctx.path_cat)
        if os.path.exists(path_cat_bak):
            os.remove(path_cat_bak)
    except IsADirectoryError as e:
        # Should not occur, it's our files
        print_debug(e)
        return False
    except FileNotFoundError as e:
        # Should not occur, it's being tested above
        print_debug(e)
        return False
    except OSError as e:
        # Faied to create file
        print_debug(e)
        return False
    except Exception as e:
        print_debug("Unexpected exception!")
        print_debug(e)
        return False
    return True


def get_unique_uuid(catalog_dict: Dict) -> str:
    """Returns a new, random UUID, which does not already exist in catalog."""

    uuid_is_unique = False
    while not uuid_is_unique:
        uuid_result = str(uuid.uuid4())
        uuid_is_unique = True
        for uuid_existing, _, _ in catalog_dict.values():
            if uuid_result == uuid_existing:
                uuid_is_unique = False
                break
    return uuid_result


def read_poliigon_categories(ctx: ScriptContext) -> bool:
    """Reads all Poliigon categories into a dict in context."""

    if not os.path.exists(ctx.path_categories):
        print_debug("Poliigon categories file missing")
        ctx.poliigon_categories = {"Brushes": [],
                                   "HDRIs": [],
                                   "Models": [],
                                   "Textures": []
                                   }
        return False

    with open(ctx.path_categories, "r") as file_categories:
        try:
            ctx.poliigon_categories = json.load(file_categories)
        except json.JSONDecodeError:
            print_debug("Poliigon's category file is corrupt!")
            return False

    ctx.poliigon_categories = ctx.poliigon_categories["poliigon"]
    return True


def get_unique_category_list(ctx: ScriptContext, asset_data: Dict) -> List[str]:
    """Returns a list of categories matching the first (alphabetically)
    matching branch in Poliigon's category tree."""

    asset_type = asset_data.get("type", "")

    if "categories" not in asset_data:
        return [asset_type]

    if asset_type not in ctx.poliigon_categories:
        print_debug("!!! Asset type not found", asset_data["name"], asset_type)
        print_debug("   Category types", list(ctx.poliigon_categories.keys()))
        return [asset_type]

    asset_categories = asset_data.get("categories", [])

    if "free" in asset_categories:
        asset_categories.remove("free")

    if asset_type == "HDRIs":
        if "HDRS" in asset_categories:
            asset_categories.remove("HDRS")
    elif asset_type in asset_categories:
        asset_categories.remove(asset_type)  # it gets prepended anyway in next step

    category_list = [asset_type]
    cat_slug = ""
    for cat in asset_categories:
        cat = cat.title()
        cat_slug += "/" + cat
        if cat_slug not in ctx.poliigon_categories[asset_type]:
            break
        category_list.append(cat)

    return category_list


def add_catalog(ctx: ScriptContext, asset_data: Dict, entity: Any) -> bool:
    """Assigns a catalog to an entity (object, collection, material, world,...).

    If needed, the catalog file will be extended with additional catalogs based
    on the categories of the asset.
    """

    catalog_dict = get_catalog_dict(ctx)

    asset_type = asset_data.get("type", "")
    asset_categories = get_unique_category_list(ctx, asset_data)

    # After this loop uuid_result contains the UUID of the leaf catalog
    for idx_cat, category in enumerate(asset_categories):
        category_path = "/".join(asset_categories[:idx_cat + 1])
        if category_path not in catalog_dict:
            uuid_result = get_unique_uuid(catalog_dict)
            catalog_dict[category_path] = (uuid_result, category_path, category)
        else:
            uuid_result, _, _ = catalog_dict[category_path]

    if not write_catalog_file(ctx, catalog_dict):
        print_debug("add_catalog(): Failed to write catalog file")
        return False

    # Finally assign the determined UUID to the entity
    entity.asset_data.catalog_id = uuid_result
    return True


def assign_asset_tags(asset_data: Dict, entity: Any, params: Dict) -> None:
    """Assigns tags to an entity (object, collection, material, world,...).

    NOTE: This function requires entity.asset_mark() to be called beforehand.

    Args:
        asset_data: An asset data dict passed down from P4B host.
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    asset_name = asset_data.get("name", "")
    asset_type = asset_data.get("type", "")

    entity.asset_data.tags.new(asset_data.get("name_beauty", ""))  # display name
    entity.asset_data.tags.new(asset_name)  # unique name
    entity.asset_data.tags.new("Poliigon")
    if "categories" in asset_data:
        for category in asset_data["categories"]:
            # TODO(Andreas): maybe we want to filter free?
            entity.asset_data.tags.new(category.title())

    if asset_type == "Brushes":
        raise NotImplementedError("Brushes not supported by Blender's Asset Browser")
    elif asset_type == "HDRIs":
        entity.asset_data.tags.new(params["size"])
        entity.asset_data.tags.new(params["size_bg"])
    elif asset_type == "Models":
        entity.asset_data.tags.new(params["size"])
        entity.asset_data.tags.new(params["lod"])
    elif asset_type == "Textures":
        entity.asset_data.tags.new(params["size"])
    else:
        raise NotImplementedError(f"Unsupported asset type: {asset_type}")


def assign_asset_preview(asset_data: Dict, entity: Any, params: Dict) -> None:
    """Assigns a preview image to an entity (object, collection, material,
    world,...).

    NOTE: This function requires entity.asset_mark() to be called beforehand.

    Args:
        asset_data: An asset data dict passed down from P4B host.
        entity: Blender's object, collection, material, ...
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    path_thumb = params["thumb"]
    if path_thumb is not None and len(path_thumb) > 2 and os.path.exists(path_thumb):
        # From: https://blender.stackexchange.com/questions/6101/poll-failed-context-incorrect-example-bpy-ops-view3d-background-image-add
        # and: https://blender.stackexchange.com/questions/245397/batch-assign-pre-existing-image-files-as-asset-previews
        if hasattr(bpy.context, "temp_override"):  # equal to bpy.app.version >= (3, 2, 0)
            with bpy.context.temp_override(id=entity):
                bpy.ops.ed.lib_id_load_custom_preview(filepath=path_thumb)
        else:
            bpy.ops.ed.lib_id_load_custom_preview({"id": entity},
                                                  filepath=path_thumb)
    else:
        # TODO(Andreas): Not working as expected
        #                Maybe https://developer.blender.org/T93893 ?
        entity.asset_generate_preview()


def assign_asset_meta_data(ctx: ScriptContext,
                           asset_data: Dict,
                           entity: Any,
                           params: Dict) -> bool:
    """Assigns all meta data (e.g. author, tags, preview, catalog...) to an
    entity (object, collection, material, world,...).

    Args:
        ctx: ScriptContext instance created upon script start
        asset_data: An asset data dict passed down from P4B host.
        entity: Blender's object, collection, material, ...
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    if hasattr(entity, "type"):
        type_label = f", type: {entity.type}"
    elif isinstance(entity, bpy.types.Material):
        type_label = f", type: Material"
    else:
        type_label = f", type: UNKNOWN"
    print_debug(f"Marking {entity.name} {type_label}")

    entity.asset_mark()

    entity.asset_data.author = "Poliigon"
    entity.asset_data.description = asset_data.get("name_beauty", "")

    try:
        assign_asset_tags(asset_data, entity, params)
    except NotImplementedError as e:
        print_debug(e)
        return False
    assign_asset_preview(asset_data, entity, params)
    if not add_catalog(ctx, asset_data, entity):
        print_debug("assign_asset_meta_data(): Failed to add catalog")
        return False

    return True


def process_brush(ctx: ScriptContext, asset_data: Dict, params: Dict) -> bool:
    """Processes a Brush asset.

    Args:
        ctx: ScriptContext instance created upon script start
        asset_data: An asset data dict passed down from P4B host.
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    # TODO(Andreas): Not supported in Blender's asset browser, yet
    return False


def process_hdri(ctx: ScriptContext, asset_data: Dict, params: Dict) -> bool:
    """Processes an HDRI asset.

    Args:
        ctx: ScriptContext instance created upon script start
        asset_data: An asset data dict passed down from P4B host.
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    if "size" not in params or "thumb" not in params:
        print_debug("Missing required parameter (size and/or thumb) to process HDRI")
        return False

    asset_name = asset_data.get("name", "")
    size = params["size"]
    size_bg = params["size_bg"]

    print_debug("process_hdri", asset_name, size)
    try:
        result = bpy.ops.poliigon.poliigon_hdri(vAsset=asset_name,
                                                vSize=size,
                                                size_bg=size_bg)
    except Exception as e:
        print_debug("HDRI ERROR")
        print_debug("    exc", e)
        return False

    if result != {"FINISHED"}:
        return False

    # Rename world,
    # otherwise the asset would appear as "World" in the Asset Browser.
    world = bpy.context.scene.world
    world.name = asset_name

    if not assign_asset_meta_data(ctx, asset_data, world, params):
        return False

    return True


def process_model(ctx: ScriptContext, asset_data: Dict, params: Dict) -> bool:
    """Processes a Model asset.

    Args:
        ctx: ScriptContext instance created upon script start
        asset_data: An asset data dict passed down from P4B host.
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    if "size" not in params or "lod" not in params or "thumb" not in params:
        print_debug("Missing required parameter (size, lod and/or thumb) to process Model")
        return False

    asset_name = asset_data.get("name", "")
    asset_type = asset_data.get("type", "")
    size = params["size"]
    lod = params["lod"]

    print_debug("process_model", asset_name, size, lod)

    path_blend = None
    for path in asset_data["files"]:
        if path.endswith(".blend"):
            path_blend = path
            break

    # TODO(Andreas): Most likely remove "LOAD" branch, when we have finally
    #                decided how to optimally handle models
    if False:  # path_blend is not None:
        print_debug("++++++ LOAD BLEND", path_blend)
        result = bpy.ops.wm.open_mainfile(filepath=path_blend,
                                          load_ui=False)
    else:
        try:
            result = bpy.ops.poliigon.poliigon_model(vAsset=asset_name,
                                                     vType=asset_type,
                                                     vSize=size,
                                                     vLod=lod,
                                                     vLinkBlend=True,
                                                     vUseCollection=True)
        except Exception as e:
            print_debug("MODEL ERROR")
            print_debug("    exc", e)
            return False

    if result != {"FINISHED"}:
        return False

    # Mark the object instancing our collection
    found = False
    error = False
    for obj in bpy.data.objects:
        if obj.type != "EMPTY":
            continue
        if obj.parent is not None:
            continue
        if not obj.name.startswith(asset_name):
            continue
        if obj.instance_collection is None:
            continue

        if assign_asset_meta_data(ctx, asset_data, obj, params):
            found = True
        else:
            error = True
            break

    return found and not error


def process_texture(ctx: ScriptContext, asset_data: Dict, params: Dict) -> bool:
    """Processes a Texture asset (including backplates and backdrops).

    Args:
        ctx: ScriptContext instance created upon script start
        asset_data: An asset data dict passed down from P4B host.
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    if "size" not in params or "thumb" not in params:
        print_debug("Missing required parameter (size and/or thumb) to process Texture")
        return False

    asset_name = asset_data.get("name", "")
    asset_type = asset_data.get("type", "")
    size = params["size"]

    print_debug("process_texture", asset_name, size)

    try:
        data = f"{asset_name}@{size}"
        result = bpy.ops.poliigon.poliigon_material(vAsset=asset_name,
                                                    vSize=size,
                                                    vData=data,
                                                    vType=asset_type)
    except Exception as e:
        print_debug("MATERIAL ERROR")
        print_debug("    exc", e)
        return False

    if result != {"FINISHED"}:
        print_debug("Operator poliigon_material returned:", result)
        return False

    found = False
    error = False
    is_backdrop = asset_name.lower().startswith("backdrop")
    is_backplate = asset_name.lower().startswith("backplate")
    if is_backdrop or is_backplate:
        # Mark backplate/backdrop object instead of material
        for obj in bpy.data.objects:
            if obj.type == "LIGHT":
                continue
            if obj.type == "CAMERA":
                continue
            if not obj.name.startswith(asset_name):
                continue
            if obj.type != "MESH":
                continue

            # "Bake" the rotation to be respected by Asset Browser
            mb = obj.matrix_basis
            if hasattr(obj.data, "transform"):
                obj.data.transform(mb)
            obj.matrix_basis.identity()

            if assign_asset_meta_data(ctx, asset_data, obj, params):
                found = True
            else:
                error = True
                print_debug("Failed to assign meta data to object:", obj.name)
                break
    else:
        for mat in bpy.data.materials:
            if not mat.name.startswith(asset_name):
                continue

            if assign_asset_meta_data(ctx, asset_data, mat, params):
                found = True
            else:
                error = True
                print_debug("Failed to assign meta data to material:", mat.name)
                break

    if not found:
        print_debug("Found no entity to mark")

    return found and not error


def process_asset(ctx: ScriptContext, asset_data: Dict, params: Dict) -> bool:
    """Creates and saves an Asset Browser-marked asset to a new blend file.

    Args:
        ctx: ScriptContext instance created upon script start
        asset_data: An asset data dict passed down from P4B host.
        params: Populated by host in function asset_browser.py:get_asset_job_parameters()
    """

    if "path_result" not in params:
        print_debug("process_asset(): Lacking result path!")
        return False
    path_result = params["path_result"]

    reset_blend()

    asset_name = asset_data.get("name", "")
    asset_type = asset_data.get("type", "")

    print_debug("process_asset()", asset_name)
    for _param, value in params.items():
        print_debug("    ", _param, value)

    if asset_type == "Brushes":
        result = process_brush(ctx, asset_data, params)
    elif asset_type == "HDRIs":
        result = process_hdri(ctx, asset_data, params)
    elif asset_type == "Models":
        result = process_model(ctx, asset_data, params)
    elif asset_type == "Textures":
        result = process_texture(ctx, asset_data, params)
    else:
        print_debug("process_asset(): Unknown asset type")
        return False

    if result:
        result = save_blend(path_result)
    return result


def cmd_asset(ctx: ScriptContext, cmd: SyncAssetBrowserCmd) -> None:
    """Handle an ASSET command"""

    asset_data = cmd.data
    result = process_asset(ctx, asset_data, cmd.params)
    if result:
        ctx.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.ASSET_OK,
                                               data=asset_data))
    else:
        ctx.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.ASSET_ERROR,
                                               data=asset_data))
    print_debug("cmd_asset exit", asset_data["name"])


def cmd_hello_ok(ctx: ScriptContext, cmd: SyncAssetBrowserCmd) -> None:
    """Handle a HELLO_OK command"""

    print_debug("cmd_hello_ok")
    ctx.queue_ack.put(SyncAssetBrowserCmd(code=SyncCmd.CMD_DONE))


def cmd_still_there(ctx: ScriptContext, cmd: SyncAssetBrowserCmd) -> None:
    """Handle a STILL_THERE command"""

    print_debug("cmd_still_there")
    bpy.ops.poliigon.get_local_asset_sync()
    ctx.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.HELLO))


def cmd_exit(ctx: ScriptContext, cmd: SyncAssetBrowserCmd) -> None:
    """Handle an EXIT command"""

    print_debug("cmd_exit")
    # Notify host, we are going to exit
    ctx.queue_send.put(SyncAssetBrowserCmd(code=SyncCmd.EXIT_ACK))
    # Tear down everything
    ctx.listener_running = False
    if ctx.thd_listener is not None:
        ctx.thd_listener.join()
    if ctx.thd_sender is not None:
        ctx.thd_sender.join()
    ctx.main_running = False


def main():
    print_debug("Hello Blender host, I am the client")

    ctx = ScriptContext()

    if not command_line_args(ctx):
        return

    if not startup(ctx):
        return

    ctx.main_running = True
    while ctx.main_running:
        try:
            cmd_recv = ctx.queue_cmd.get(timeout=1.0)
            ctx.queue_cmd.task_done()
        except queue.Empty:
            continue

        if cmd_recv is None:
            continue

        if cmd_recv.code == SyncCmd.EXIT:
            cmd_exit(ctx, cmd_recv)
        elif cmd_recv.code == SyncCmd.ASSET:
            cmd_asset(ctx, cmd_recv)
        elif cmd_recv.code == SyncCmd.STILL_THERE:
            cmd_still_there(ctx, cmd_recv)
        elif cmd_recv.code == SyncCmd.HELLO_OK:
            cmd_hello_ok(ctx, cmd_recv)

    print_debug("Subprocess exit")


if __name__ == "__main__":
    main()
