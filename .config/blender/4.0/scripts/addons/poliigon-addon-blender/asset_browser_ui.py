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
from typing import Optional

import bpy

from .toolbox import cTB
from . import reporting
from .asset_browser import (get_num_selected_assets,
                            is_asset_browser,
                            is_only_poliigon_selected,
                            is_poliigon_library,
                            t_status_bar_update)


def build_asset_browser_progress(ui: bpy.types.Panel,
                                 context: bpy.context,
                                 layout: Optional[bpy.types.UILayout] = None,
                                 show_label: bool = True,
                                 show_cancel: bool = True,
                                 show_second_line: bool = False) -> None:
    if cTB.num_asset_browser_jobs == 0:
        return

    if layout is None:
        layout = ui.layout

    num_in_queue = cTB.queue_send.qsize()
    num_jobs_done = cTB.num_asset_browser_jobs - num_in_queue
    progress = num_jobs_done / cTB.num_asset_browser_jobs
    done = cTB.num_jobs_error + cTB.num_jobs_ok == cTB.num_asset_browser_jobs

    layout.separator()

    col = layout.column()
    row = col.row(align=True)

    if show_label and done:
        row.label(text="Asset Browser Synchronization :")
    elif show_label and not done:
        row.label(text=f"Asset Browser Synchronization : {progress:.1%}")

    if done:
        if cTB.num_jobs_error:
            text = (f"Finished {cTB.num_asset_browser_jobs} assets, "
                    f"{cTB.num_jobs_error} errors")
            row.label(text=text,
                      icon="ERROR")
        else:
            text = f"Successfully finished {cTB.num_asset_browser_jobs} assets"
            row.label(text=text,
                      icon="CHECKMARK")
        return

    split = row.split(factor=progress, align=True)
    op = split.operator(
        "poliigon.poliigon_setting", text="", emboss=1, depress=1
    )
    op.vMode = "none"
    op.vTooltip = (f"Processing assets: {num_jobs_done} of "
                   f"{cTB.num_asset_browser_jobs} assets done")

    op = split.operator(
        "poliigon.poliigon_setting", text="", emboss=1, depress=0
    )
    op.vMode = "none"
    op.vTooltip = (f"Processing assets: {num_jobs_done} of "
                   f"{cTB.num_asset_browser_jobs} assets done")

    if show_cancel:
        op = row.operator(
            "poliigon.cancel_asset_browser",
            text="",
            emboss=True,
            depress=0,
            icon="X"
        )
    if cTB.asset_browser_jobs_cancelled:
        row.enabled = False

    if show_second_line:
        text = (f"Processed {num_jobs_done} of {cTB.num_asset_browser_jobs} "
                "assets.")
        col.label(text=text)


class POLIIGON_PT_sidebar_left(bpy.types.Panel):
    bl_label = "Poliigon"
    bl_space_type = "FILE_BROWSER"
    bl_region_type = "TOOLS"
    bl_options = {"HIDE_HEADER"}

    view_screen_tracked = False

    @classmethod
    def poll(self, context):
        if not cTB.is_logged_in():
            return False
        if not is_asset_browser(context):
            return False
        if not is_poliigon_library(context, incl_all_libs=False):
            return False
        return True

    @reporting.handle_draw()
    def draw(self, context):
        cTB._api._mp_relevant = True

        if not self.view_screen_tracked:
            # TODO(patrick): value not retained, re-triggering on future draws
            self.view_screen_tracked = True
            cTB.track_screen("blend_browser_lib")

        layout = self.layout
        box = layout.box()
        col = box.column(align=True)

        name_is_set = cTB.prefs.asset_browser_library_name != ""
        directory_is_set = cTB.vSettings["library"] != ""
        sync_options_enabled = name_is_set and directory_is_set

        if cTB.num_asset_browser_jobs == 0 and not cTB.lock_client_start.locked():
            col.label(text="Poliigon assets:")
            row_manual_sync = col.row(align=1)
            op_manual_sync = row_manual_sync.operator(
                "poliigon.update_asset_browser",
                text="Synchronize Local Assets",
                emboss=True,
                icon="FILE_REFRESH",
            )
            op_manual_sync.asset_name = ""  # empty string == all local assets
            row_manual_sync.enabled = sync_options_enabled
        else:
            col.label(text="Poliigon Asset Browser Synchronization")
            build_asset_browser_progress(self,
                                         context,
                                         col,
                                         show_label=False,
                                         show_second_line=True)


class POLIIGON_PT_sidebar_right(bpy.types.Panel):
    bl_label = "Poliigon in Asset Browser"
    bl_space_type = "FILE_BROWSER"
    bl_region_type = "TOOL_PROPS"  # right side panel
    bl_options = {"HEADER_LAYOUT_EXPAND"}

    view_screen_tracked = False

    @classmethod
    def poll(self, context):
        if not cTB.is_logged_in():
            return False
        if not is_asset_browser(context):
            return False
        if not is_poliigon_library(context):
            return False
        if not is_only_poliigon_selected(context):
            return False
        return True

    @reporting.handle_draw()
    def draw(self, context):
        if not is_poliigon_library(context):
            return

        if not self.view_screen_tracked:
            cTB.track_screen("blend_browser_import")
            self.view_screen_tracked = True

        num_selected = get_num_selected_assets(context)
        if num_selected == 1:
            label_import = "Import Asset (TODO)"
            label_reprocess = "Re-process Asset"
        elif num_selected > 1:
            label_import = f"Import {num_selected} Assets (TODO)"
            label_reprocess = f"Re-process {num_selected} Assets"
        else:
            label_import = "No Asset Selected"
            label_reprocess = "Re-process Asset"

        layout = self.layout
        col = layout.column()
        row = col.row()
        row.operator("poliigon.asset_browser_reprocess",
                     text=label_reprocess,
                     icon="FILE_REFRESH")

        col.separator()

        if not (cTB.env.env_name and "dev" in cTB.env.env_name.lower()):
            return

        # TODO(Andreas): Import button currently in dev environment, only
        row = col.row(align=True)
        row.operator(
            "poliigon.asset_browser_import",
            text=label_import,
            emboss=True,
        )
        row.operator(
            "poliigon.asset_browser_quick_menu",
            text="",
            icon="TRIA_DOWN",
        )


classes_prod = (
    POLIIGON_PT_sidebar_left,
)

classes_dev = (
    POLIIGON_PT_sidebar_left,
    POLIIGON_PT_sidebar_right
)

if cTB.env.env_name and "dev" in cTB.env.env_name.lower():
    classes = classes_dev
else:
    classes = classes_prod


def register():
    for cls in classes:
        bpy.utils.register_class(cls)

    bpy.types.STATUSBAR_HT_header.prepend(build_asset_browser_progress)


def unregister():
    if bpy.app.timers.is_registered(t_status_bar_update):
        bpy.app.timers.unregister(t_status_bar_update)

    bpy.types.STATUSBAR_HT_header.remove(build_asset_browser_progress)

    for cls in reversed(classes):
        bpy.utils.unregister_class(cls)
