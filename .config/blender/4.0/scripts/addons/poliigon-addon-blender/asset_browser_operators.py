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

import threading
from bpy.types import Operator
from bpy.props import (
    BoolProperty,
    StringProperty,
)

import bpy

from .ui import show_quick_menu
from .toolbox import cTB
from . import reporting
# from .asset_browser import *
from . import asset_browser as ab


# https://blender.stackexchange.com/questions/249837/how-do-i-get-the-selected-assets-in-the-asset-browser-using-the-api
class POLIIGON_OT_asset_browser_import(Operator):
    bl_idname = "poliigon.asset_browser_import"
    bl_label = "Import Selected Assets"
    bl_space_type = "FILE_BROWSER"

    @classmethod
    def poll(cls, context):
        is_poliigon_lib = ab.is_poliigon_library(context)
        assets_selected = ab.get_num_selected_assets(context) > 0
        return is_poliigon_lib and assets_selected

    @classmethod
    def description(cls, context, properties):
        num_selected = ab.get_num_selected_assets(context)
        if num_selected > 0:
            return "Import selected assets (default parameters)"
        else:
            return "No asset selected.\nPlease, select an asset"

    @reporting.handle_operator(silent=True)
    def execute(self, context):
        if not ab.is_poliigon_library(context):
            # As the operator should be shown for Poliigon Library, only
            # we shouldn't be here
            error_msg = ("POLIIGON_OT_asset_browser_import(): "
                         "Poliigon library not selected!")
            reporting.capture_message(
                "asset_browser_lib_not_sel", error_msg, "error")
            return {"CANCELLED"}

        asset_files = ab.get_selected_assets(context)

        for _asset_file in asset_files:
            asset_name = ab.get_asset_name_from_browser_asset(_asset_file)
            asset_data = ab.get_asset_data_from_browser_asset(
                _asset_file, asset_name)
            if asset_data is None:
                error_msg = ("POLIIGON_OT_asset_browser_import(): "
                             f"Asset {asset_name} not found!")
                reporting.capture_message(
                    "asset_browser_asset_not_found", error_msg, "error")
                print(error_msg)
                # TODO(Andreas): user notification
                continue
            asset_type = asset_data["type"]
            if asset_type == "Brushes":
                error_msg = ("POLIIGON_OT_asset_browser_import(): "
                             f"Found unsupported brush asset: {asset_name}")
                reporting.capture_message(
                    "asset_browser_brush_unsupported", error_msg, "error")
                print(error_msg)
                # TODO(Andreas): user notification
                continue
            elif asset_type == "HDRIs":
                # TODO(Andreas): Do actual import
                pass
            elif asset_type == "Models":
                # TODO(Andreas): Do actual import
                pass
            elif asset_type == "Textures":
                # TODO(Andreas): Do actual import
                pass
            else:
                error_msg = ("POLIIGON_OT_asset_browser_import():"
                             f" Unexpected asset type: {asset_name} {asset_type}")
                reporting.capture_message(
                    "asset_browser_unexpected_type", error_msg, "error")
                print(error_msg)
                # TODO(Andreas): user notification
                continue

        return {"FINISHED"}


# https://blender.stackexchange.com/questions/249837/how-do-i-get-the-selected-assets-in-the-asset-browser-using-the-api
class POLIIGON_OT_asset_browser_quick_menu(Operator):
    bl_idname = "poliigon.asset_browser_quick_menu"
    bl_label = "Show additional import options"
    bl_space_type = "FILE_BROWSER"

    @classmethod
    def poll(cls, context):
        is_poliigon_lib = ab.is_poliigon_library(context)
        one_asset_selected = ab.get_num_selected_assets(context) == 1
        return is_poliigon_lib and one_asset_selected

    @classmethod
    def description(cls, context, properties):
        num_selected = ab.get_num_selected_assets(context)
        if num_selected == 1:
            return "Show additional import options"
        elif num_selected == 0:
            return "No asset selected.\nPlease, select a single asset"
        else:
            return "Multiple assets selected.\nPlease, select a single asset, only"

    @reporting.handle_operator(silent=True)
    def execute(self, context):
        if not ab.is_poliigon_library(context):
            # As the operator should be shown for Poliigon Library
            # we shouldn't be here
            error_msg = ("POLIIGON_OT_asset_browser_quick_menu(): "
                         "Poliigon library not selected!")
            reporting.capture_message(
                "asset_browser_lib_not_sel", error_msg, "error")
            return {"CANCELLED"}

        # poll() makes sure, there's exactly one
        asset_file = ab.get_selected_assets(context)[0]

        asset_name = ab.get_asset_name_from_browser_asset(asset_file)
        asset_data = ab.get_asset_data_from_browser_asset(
            asset_file, asset_name)

        if asset_data is None:
            error_msg = ("POLIIGON_OT_asset_browser_import(): "
                         f"Asset {asset_name} not found!")
            reporting.capture_message(
                "asset_browser_asset_not_found", error_msg, "error")
            self.report({"ERROR"}, f"Asset {asset_name} not found!")
            return {"CANCELLED"}

        show_quick_menu(cTB,
                        asset_name=asset_name,
                        asset_id=asset_data["id"],
                        asset_type=asset_data["type"],
                        sizes=asset_data["sizes"])
        return {"FINISHED"}


class POLIIGON_OT_asset_browser_reprocess(Operator):
    bl_idname = "poliigon.asset_browser_reprocess"
    bl_label = "Show additional import options"
    bl_space_type = "FILE_BROWSER"

    @classmethod
    def poll(cls, context):
        if not ab.is_asset_browser(context):
            return False
        if not ab.is_poliigon_library(context):
            return False
        if not ab.is_only_poliigon_selected(context):
            return False
        if ab.get_num_selected_assets(context) == 0:
            return False
        return True

    @classmethod
    def description(cls, context, properties):
        num_selected = ab.get_num_selected_assets(context)
        if num_selected == 0:
            return "No asset selected.\nPlease, select a single asset"
        else:
            return "Re-process selected assets"

    @reporting.handle_operator(silent=True)
    def execute(self, context):
        if not ab.is_poliigon_library(context):
            # As the operator should be shown for Poliigon Library
            # we shouldn't be here
            error_msg = ("POLIIGON_OT_asset_browser_reprocess(): "
                         "Poliigon library not selected!")
            reporting.capture_message(
                "asset_browser_reproc_not_sel", error_msg, "error")
            return {"CANCELLED"}

        asset_files = ab.get_selected_assets(context)

        for asset_file in asset_files:
            asset_name = ab.get_asset_name_from_browser_asset(asset_file)
            asset_data = ab.get_asset_data_from_browser_asset(
                asset_file, asset_name)

            if asset_data is None:
                error_msg = ("POLIIGON_OT_asset_browser_reprocess(): "
                             f"Asset {asset_name} not found!")
                reporting.capture_message(
                    "asset_browser_reproc_asset_missing", error_msg, "error")
                self.report({"ERROR"}, f"Asset {asset_name} not found!")
                continue

            bpy.ops.poliigon.update_asset_browser(asset_name=asset_name,
                                                  force=True)

        return {"FINISHED"}


class POLIIGON_OT_update_asset_browser(Operator):
    bl_idname = "poliigon.update_asset_browser"
    bl_label = "Sync Local Assets"
    bl_category = "Poliigon"
    bl_description = "Synchronize local assets with Asset Browser"
    bl_options = {"INTERNAL"}

    asset_name: StringProperty(options={"HIDDEN"}, default="")
    force: BoolProperty(options={"HIDDEN"}, default=False)

    @reporting.handle_operator(silent=True)
    def execute(self, context):
        if bpy.app.version < (3, 0):
            self.report(
                {"ERROR"},
                "Asset browser not available in this blender version")
            return {'CANCELLED'}

        bpy.ops.poliigon.get_local_asset_sync()

        if ab.create_poliigon_library() is None:
            print("HOST: No Poliigon library in Asset Browser!")
            error_msg = "No Poliigon library in Asset."
            reporting.capture_message(
                "asset_browser_no_polii_lib", error_msg, "error")
            self.report({"ERROR"}, error_msg)
            return {"CANCELLED"}

        thd_init_sync = threading.Thread(
            target=ab.thread_initiate_asset_synchronization,
            args=(self.asset_name, self.force, ))
        thd_init_sync.start()
        cTB.vThreads.append(thd_init_sync)

        return {"FINISHED"}


class POLIIGON_OT_cancel_asset_browser_sync(Operator):
    bl_idname = "poliigon.cancel_asset_browser"
    bl_label = "Cancel Asset Sync"
    bl_category = "Poliigon"
    bl_description = "Cancel synchronization of local assets with the Asset Browser"
    bl_options = {"INTERNAL"}

    @reporting.handle_operator(silent=True)
    def execute(self, context):
        cTB.asset_browser_jobs_cancelled = True
        return {"FINISHED"}


classes = (
    POLIIGON_OT_update_asset_browser,
    POLIIGON_OT_cancel_asset_browser_sync,
    POLIIGON_OT_asset_browser_import,
    POLIIGON_OT_asset_browser_quick_menu,
    POLIIGON_OT_asset_browser_reprocess,
)


def register():
    for cls in classes:
        bpy.utils.register_class(cls)


def unregister():
    for cls in reversed(classes):
        bpy.utils.unregister_class(cls)
