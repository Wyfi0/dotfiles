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

import bpy

from .toolbox import cTB
from .asset_browser_ui import build_asset_browser_progress
from .utils import shorten_label
from . import reporting

THUMB_SIZES = ["Tiny", "Small", "Medium", "Large", "Huge"]


def optin_update(self, context):
    """Update the optin settings."""
    prefs = bpy.context.preferences.addons.get(__package__, None)
    reporting.set_optin(prefs.preferences.reporting_opt_in)


def verbose_update(self, context):
    """Clear out print cache, which could prevent new, near-term prinouts."""
    cTB._cached_print.cache_clear()


def get_preferences_width(context, subtract_offset: bool = True) -> float:
    """Returns width of user preferences dialog's main region/draw area"""

    width_win = 1
    for area in context.screen.areas:
        if area.type != "PREFERENCES":
            continue

        for region in area.regions:
            if region.type == "WINDOW":
                width_win = region.width
                break
        break

    if subtract_offset:
        width_win = width_win - 25 - 20

    width_win = max(width_win, 1)  # To avoid div by zero errors
    return width_win


def draw_prefs_section_header(self,
                              text: str,
                              param: str,
                              tooltip_hide: str,
                              tooltip_show: str,
                              have_column: bool = False
                              ) -> None:
    if param not in cTB.vSettings:
        msg = f"Section header {param} not found in settings"
        reporting.capture_message("settings_error", msg, "error")

    is_open = cTB.vSettings.get(param, True)
    icon = "DISCLOSURE_TRI_DOWN" if is_open else "DISCLOSURE_TRI_RIGHT"

    if have_column:
        col = self.layout.column(align=1)
        box = col.box()
    else:
        col = None
        box = self.layout.box()

    op = box.operator(
        "poliigon.poliigon_setting",
        text=text,
        icon=icon,
        emboss=0,
    )
    op.vMode = param
    if cTB.vSettings[param]:
        op.vTooltip = tooltip_hide
    else:
        op.vTooltip = tooltip_show

    return box, col


def draw_prefs_section_header_prop(self,
                                   text: str,
                                   prop_name: str,
                                   do_show: bool,
                                   have_column: bool = False
                                   ) -> None:
    icon = "DISCLOSURE_TRI_DOWN" if do_show else "DISCLOSURE_TRI_RIGHT"

    if have_column:
        col = self.layout.column(align=1)
        box = col.box()
    else:
        col = None
        box = self.layout.box()

    box.prop(self, prop_name, emboss=False, icon=icon, text=text)

    return box, col


def draw_library_prefs(self) -> bool:
    box = self.layout.box().column()
    col = box.column()

    col.label(text="Library :")

    op = col.operator(
        "poliigon.poliigon_library",
        icon="FILE_FOLDER",
        text=cTB.vSettings["library"],
    )
    op.vMode = "update_library"
    op.directory = cTB.vSettings["library"]
    op.vTooltip = "Set Default Poliigon Library Directory"

    if not os.path.exists(cTB.vSettings["library"]):
        col.label(text="(Poliigon Library not set.)", icon="ERROR")
        return False

    col.separator()
    return True


def draw_additional_dirs_prefs(self) -> None:
    additional_dirs = cTB.vSettings["add_dirs"]
    text = f"{len(additional_dirs)} Additional Directories"
    box, _ = draw_prefs_section_header(self,
                                       text=text,
                                       param="show_add_dir",
                                       tooltip_hide="Hide Additional Directories",
                                       tooltip_show="Show Additional Directories")
    if not cTB.vSettings["show_add_dir"]:
        return

    col = box.column()

    for directory in cTB.vSettings["add_dirs"]:
        row = col.row(align=1)
        check = directory not in cTB.vSettings["disabled_dirs"]
        op = row.operator(
            "poliigon.poliigon_setting",
            text="",
            depress=check,
            emboss=False,
            icon="CHECKBOX_HLT" if check else "CHECKBOX_DEHLT",
        )
        op.vMode = f"disable_dir_{directory}"
        if check:
            op.vTooltip = "Disable Additional Directory"
        else:
            op.vTooltip = "Enable Additional Directory"

        row.label(text=directory)

        op = row.operator("poliigon.poliigon_setting", text="", icon="TRASH")
        op.vMode = f"del_dir_{directory}"
        op.vTooltip = "Remove Additional Directory"

        col.separator()

    row = col.row(align=1)
    op = row.operator(
        "poliigon.poliigon_directory",
        text="Add Additional Directory",
        icon="ADD",
    )
    op.directory = cTB.vSettings["library"]
    op.vTooltip = "Add Additional Asset Directory"

    col.separator()


def draw_asset_browser_prefs(context, poliigon_preferences):
    """Draws preferencees related to Blender's Asset Browser."""

    if bpy.app.version < (3, 0):
        return  # Not available before blender 3.0.

    layout = poliigon_preferences.layout
    col_asset_browser = layout.column(align=True)
    box = col_asset_browser.box()

    op = box.operator(
        "poliigon.poliigon_setting",
        text="Asset Browser Preferences",
        icon="DISCLOSURE_TRI_DOWN"
        if cTB.vSettings["show_asset_browser_prefs"]
        else "DISCLOSURE_TRI_RIGHT",
        emboss=0,
    )
    op.vMode = "show_asset_browser_prefs"
    if cTB.vSettings["show_asset_browser_prefs"]:
        op.vTooltip = "Hide Asset Browser Preferences"
    else:
        op.vTooltip = "Show Asset Browser Preferences"

    if cTB.vSettings["show_asset_browser_prefs"]:
        name_is_set = cTB.prefs.asset_browser_library_name != ""
        directory_is_set = cTB.vSettings["library"] != ""
        sync_options_enabled = name_is_set and directory_is_set

        col = box.column()

        width = get_preferences_width(context, subtract_offset=False)
        label = (
            "This addon can generate blend files for the Blender Asset "
            "Browser (except for Brushes). You can then use the Poliigon "
            "Library in the Asset Browser.")
        cTB.f_Label(width - 26 * cTB.get_ui_scale(), label, col)

        col.separator()

        col.prop(poliigon_preferences, "asset_browser_library_name")

        if cTB.env.env_name and "dev" in cTB.env.env_name.lower():
            row_mode = col.row(align=True)
            row_mode.prop(poliigon_preferences, "asset_browser_mode")
            row_mode.enabled = sync_options_enabled

        col.separator()

        row_manual_sync = col.row(align=True)
        row_manual_sync.label(text="Manually Start Synchronization:")

        if cTB.num_asset_browser_jobs == 0 and not cTB.lock_client_start.locked():
            op_manual_sync = row_manual_sync.operator(
                "poliigon.update_asset_browser",
                text="Synchronize Local Assets",
                emboss=True,
                icon="FILE_REFRESH",
            )
            op_manual_sync.asset_name = ""  # empty string == all local assets
            row_manual_sync.enabled = sync_options_enabled
        else:
            build_asset_browser_progress(
                None, context, row_manual_sync, show_label=False, show_second_line=True)

        col.separator()


def draw_display_prefs(self) -> None:
    box, _ = draw_prefs_section_header(self,
                                       text="Display Preferences",
                                       param="show_display_prefs",
                                       tooltip_hide="Hide Display Preferences",
                                       tooltip_show="Show Display Preferences")
    if not cTB.vSettings["show_display_prefs"]:
        return

    col = box.column()

    col.label(text="Thumbnail Size :")
    row = col.row(align=False)
    for size in THUMB_SIZES:
        op = row.operator(
            "poliigon.poliigon_setting",
            text=size,
            depress=cTB.vSettings["thumbsize"] == size,
        )
        op.vMode = f"thumbsize@{size}"
        op.vTooltip = f"Show {size} Thumbnails"

    col.separator()

    col.label(text="Assets Per Page :")
    row = col.row(align=False)
    for num_thumbs in [6, 8, 10, 20]:
        op = row.operator(
            "poliigon.poliigon_setting",
            text=str(num_thumbs),
            depress=cTB.vSettings["page"] == num_thumbs,
        )
        op.vMode = f"page@{num_thumbs}"
        op.vTooltip = f"Show {num_thumbs} Assets per Page"

    row = col.row()
    row.scale_y = 0.25
    row.label(text="")
    row = col.row()
    split = row.split(factor=0.76)
    col_left = split.column()
    col_left.label(
        text="Press Refresh Data to reload icons and reset addon data:")
    col_right = split.column()
    col_right.operator("poliigon.refresh_data", icon="FILE_REFRESH")

    col.separator()


def draw_download_prefs_texture_resolution(self, box, prefs_width) -> None:
    col = box.column()

    col.label(text="Default Texture Resolution :")
    grid = col.grid_flow(
        row_major=1,
        columns=int((prefs_width - 20) / 40),
        even_columns=1,
        even_rows=1,
        align=0,
    )
    for size in ["1K", "2K", "3K", "4K", "6K", "8K", "16K"]:
        op = grid.operator(
            "poliigon.poliigon_setting",
            text=size,
            depress=(size == cTB.vSettings["res"]),
        )
        op.vMode = f"default_res_{size}"
        op.vTooltip = "The default Resolution to use for Texture Assets"

    col.separator()


def draw_download_prefs_blend_file(self, col) -> None:
    col.separator()

    row = col.row(align=1)
    row.separator()
    op = row.operator(
        "poliigon.poliigon_setting",
        text="",
        depress=cTB.vSettings["download_prefer_blend"],
        emboss=False,
        icon="CHECKBOX_HLT" if cTB.vSettings["download_prefer_blend"] else "CHECKBOX_DEHLT",
    )
    op.vMode = "download_prefer_blend"
    op.vTooltip = "Prefer .blend file downloads"
    row.label(text=" Download + Import .blend Files (over FBX)")

    row = col.row(align=1)
    row.separator()
    op = row.operator(
        "poliigon.poliigon_setting",
        text="",
        depress=cTB.vSettings["download_link_blend"],
        emboss=False,
        icon="CHECKBOX_HLT" if cTB.vSettings["download_link_blend"] else "CHECKBOX_DEHLT",
    )
    op.vMode = "download_link_blend"
    op.vTooltip = "Link blend files instead of appending"
    row.label(text=" Link .blend Files (n/a if any LOD is selected)")
    row.enabled = cTB.vSettings["download_prefer_blend"]
    row.separator()

    col.separator()


def draw_download_prefs_model_resolution(self, col, prefs_width) -> None:
    col.label(text="Default Model Resolution :")
    grid = col.grid_flow(
        row_major=1,
        columns=int((prefs_width - 20) / 40),
        even_columns=1,
        even_rows=1,
        align=0,
    )
    for size in ["1K", "2K", "3K", "4K", "6K", "8K", "16K"]:
        vOp = grid.operator(
            "poliigon.poliigon_setting",
            text=size,
            depress=(size == cTB.vSettings["mres"]),
        )
        vOp.vMode = f"default_mres_{size}"
        vOp.vTooltip = "The default Texture Resolution to use for Model Assets"

    col.separator()
    col.separator()


def draw_download_prefs_model_lod(self, col, prefs_width) -> None:
    download_lods = cTB.vSettings["download_lods"]
    row = col.row(align=1)
    row.separator()
    op = row.operator(
        "poliigon.poliigon_setting",
        text="",
        depress=download_lods,
        emboss=False,
        icon="CHECKBOX_HLT" if download_lods else "CHECKBOX_DEHLT",
    )
    op.vMode = "download_lods"
    op.vTooltip = "Download Model LODs"
    row.label(text=" Download Model LODs")
    row.separator()

    col.separator()

    col_lod = col.column()
    col_lod.enabled = cTB.vSettings["download_lods"]

    col_lod.label(text="Default LOD to load (NONE imports .blend, otherwise loads FBX) :")
    grid = col_lod.grid_flow(
        row_major=1,
        columns=int((prefs_width - 20) / 50),
        even_columns=1,
        even_rows=1,
        align=0,
    )
    lod_list = ["NONE", "LOD0", "LOD1", "LOD2", "LOD3", "LOD4"]
    for lod in lod_list:
        op = grid.operator(
            "poliigon.poliigon_setting",
            text=lod,
            depress=(lod in cTB.vSettings["lod"]),
        )
        op.vMode = f"default_lod_{lod}"
        op.vTooltip = "The default LOD to use for Model Assets"

    col.separator()


def draw_download_prefs_hdri_resolutions(self, col_download, prefs_width) -> None:
    col = col_download.box().column()

    col.separator()

    col.label(text="Default HDRI Lighting Resolution :")
    grid = col.grid_flow(
        row_major=1,
        columns=int((prefs_width - 20) / 40),
        even_columns=1,
        even_rows=1,
        align=0,
    )
    for size in cTB.HDRI_RESOLUTIONS:
        op = grid.operator(
            "poliigon.poliigon_setting",
            text=size,
            depress=(size == cTB.vSettings["hdri"]),
        )
        op.vMode = f"default_hdri_{size}"
        op.vTooltip = "The default Resolution to use for HDRI Lighting"

    # .....................................................................

    col.separator()

    hdri_use_jpg_bg = cTB.vSettings["hdri_use_jpg_bg"]

    row = col.row(align=1)
    row.separator()
    op = row.operator(
        "poliigon.poliigon_setting",
        text="",
        depress=hdri_use_jpg_bg,
        emboss=False,
        icon="CHECKBOX_HLT" if hdri_use_jpg_bg else "CHECKBOX_DEHLT",
    )
    op.vMode = "hdri_use_jpg_bg"
    op.vTooltip = "Use different resolution .jpg for display in background"
    row.label(text=" Use JPG for background")

    # .....................................................................

    col.label(text="Default HDRI Background Resolution :")
    grid = col.grid_flow(
        row_major=1,
        columns=int((prefs_width - 20) / 40),
        even_columns=1,
        even_rows=1,
        align=0,
    )
    grid.enabled = hdri_use_jpg_bg

    idx_res_light = cTB.HDRI_RESOLUTIONS.index(cTB.vSettings["hdri"])

    col_no_1k_button = grid.column()
    for size in cTB.HDRI_RESOLUTIONS[1:]:
        col_button = grid.column()
        col_button.enabled = cTB.HDRI_RESOLUTIONS.index(size) > idx_res_light
        op = col_button.operator(
            "poliigon.poliigon_setting",
            text=size,
            depress=(size == cTB.vSettings["hdrib"]),
        )
        op.vMode = f"default_hdrib_{size}"
        op.vTooltip = "The default Resolution to use for HDRI Backgrounds"

    col.separator()


def draw_download_prefs_brush_resolutions(self, col_download, prefs_width) -> None:
    if self.any_owned_brushes == "No Brushes":
        return

    col = col_download.box().column()

    col.separator()

    col.label(text="Default Brush Resolution :")
    grid = col.grid_flow(
        row_major=1,
        columns=int((prefs_width - 20) / 40),
        even_columns=1,
        even_rows=1,
        align=0,
    )
    for size in ["1K", "2K", "3K", "4K"]:
        op = grid.operator(
            "poliigon.poliigon_setting",
            text=size,
            depress=(size in cTB.vSettings["brush"]),
        )
        op.vMode = f"default_brush_{size}"
        op.vTooltip = "The default Resolution to use for Brushes"

    col.separator()


def draw_download_prefs_purchase(self, col_download) -> None:
    col = col_download.box().column()

    col.separator()

    auto_download = cTB.vSettings["auto_download"]

    col.label(text="Purchase Preferences :")

    row = col.row(align=1)
    row.separator()
    op = row.operator(
        "poliigon.poliigon_setting",
        text="",
        depress=auto_download,
        emboss=False,
        icon="CHECKBOX_HLT" if auto_download else "CHECKBOX_DEHLT",
    )
    op.vMode = "auto_download"
    op.vTooltip = "Auto-Download Assets on Purchase"
    row.label(text=" Auto-Download Assets on Purchase")
    row.separator()

    col.separator()


def draw_download_prefs_import(self, col_download) -> None:
    col = col_download.box().column()

    col.separator()

    col.label(text="Import Preferences :")

    row = col.row(align=1)
    row.separator()
    icon = "CHECKBOX_HLT" if self.use_micro_displacements else "CHECKBOX_DEHLT"
    row.alignment = "LEFT"
    row.prop(self, "use_micro_displacements", icon=icon, emboss=False)
    row.separator()

    row = col.row(align=1)
    row.separator()
    op = row.operator(
        "poliigon.poliigon_setting",
        text="",
        depress=cTB.vSettings["use_16"],
        emboss=False,
        icon="CHECKBOX_HLT" if cTB.vSettings["use_16"] else "CHECKBOX_DEHLT",
    )
    op.vMode = "use_16"
    op.vTooltip = "Use 16 bit Maps if available"
    row.label(text=" Use 16 bit Maps")
    row.separator()


def draw_download_prefs(self, context) -> None:
    box, col_download = draw_prefs_section_header(
        self,
        text="Asset Preferences",
        param="show_default_prefs",
        tooltip_hide="Hide Download Preferences",
        tooltip_show="Show Download Preferences",
        have_column=True
    )
    if not cTB.vSettings["show_default_prefs"]:
        return

    prefs_width = get_preferences_width(context)

    draw_download_prefs_texture_resolution(self, box, prefs_width)

    col = col_download.box().column()
    draw_download_prefs_blend_file(self, col)
    draw_download_prefs_model_resolution(self, col, prefs_width)
    draw_download_prefs_model_lod(self, col, prefs_width)

    draw_download_prefs_hdri_resolutions(self, col_download, prefs_width)

    draw_download_prefs_brush_resolutions(self, col_download, prefs_width)

    draw_download_prefs_purchase(self, col_download)

    draw_download_prefs_import(self, col_download)


def draw_updater_prefs(self) -> None:
    if cTB.updater.update_ready:
        text = f"Update available! {cTB.updater.update_data.version}"
    else:
        text = "Addon Updates"
    box, _ = draw_prefs_section_header_prop(self,
                                            text=text,
                                            prop_name="show_updater_prefs",
                                            do_show=self.show_updater_prefs,
                                            have_column=True)
    if not self.show_updater_prefs:
        return

    col = box.column()

    colrow = col.row(align=True)
    rsplit = colrow.split(factor=0.5)
    subcol = rsplit.column()
    row = subcol.row(align=True)
    row.scale_y = 1.5

    # If already checked for update, show a refresh button (no label)
    if cTB.updater.update_ready is not None:
        row.operator("poliigon.check_update",
                     text="", icon="FILE_REFRESH")

    subcol = row.column(align=True)
    if cTB.updater.is_checking:
        subcol.operator("poliigon.check_update",
                        text="Checking...")
        subcol.enabled = False
    elif cTB.updater.update_ready is True:
        btn_label = f"Update ready: {cTB.updater.update_data.version}"
        op = subcol.operator(
            "poliigon.poliigon_link",
            text=btn_label,
        )
        op.vMode = "notify@{}@{}@{}".format(
            cTB.updater.update_data.url,
            "Install Update",
            "UPDATE_READY_MANUAL_INSTALL_PREFERENCES")
        op.vTooltip = "Download the new update from website"
    elif cTB.updater.update_ready is False:
        subcol.operator("poliigon.check_update",
                        text="No updates available")
        subcol.enabled = False
    else:  # cTB.updater.update_ready is None
        subcol.operator("poliigon.check_update",
                        text="Check for update")

    # Display user preference option for auto update.
    subcol = rsplit.column()
    subcol.scale_y = 0.8
    subcol.prop(self, "auto_check_update")

    # Next row, show time since last check.
    if cTB.updater.last_check:
        time = cTB.updater.last_check
        last_update = f"Last check: {time}"
    else:
        last_update = "(no recent check for update)"
    subcol.label(text=last_update)


def draw_legal_prefs(self) -> None:
    self.layout.prop(self, "verbose_logs")
    self.layout.prop(self, "reporting_opt_in")
    row = self.layout.row(align=True)
    op = row.operator(
        "poliigon.poliigon_link",
        text="Terms & Conditions",
    )
    op.vTooltip = "Open Terms & Conditions"
    op.vMode = "terms"

    op = row.operator(
        "poliigon.poliigon_link",
        text="Privacy Policy",
    )
    op.vTooltip = "Open Privacy Policy"
    op.vMode = "privacy"


def draw_non_prod_environment(self) -> None:
    if not cTB.env.env_name or "prod" in cTB.env.env_name.lower():
        return

    self.layout.alert = True
    msg = f"Active environment: {cTB.env.env_name}, API: {cTB.env.api_url}"
    self.layout.label(text=msg, icon="ERROR")
    self.layout.alert = False


def f_BuildSettings(self, context):
    dbg = 0
    cTB.print_separator(dbg, "f_BuildSettings")

    cTB._api._mp_relevant = True  # flag in request's meta data for Mixpanel

    library_exists = draw_library_prefs(self)
    if not library_exists:
        return

    draw_additional_dirs_prefs(self)

    draw_asset_browser_prefs(context, self)

    draw_display_prefs(self)

    draw_download_prefs(self, context)

    draw_updater_prefs(self)

    draw_legal_prefs(self)

    draw_non_prod_environment(self)


class PoliigonPreferences(bpy.types.AddonPreferences):
    bl_idname = __package__
    scriptdir = bpy.path.abspath(os.path.dirname(__file__))

    reporting_opt_in: bpy.props.BoolProperty(
        name="Share addon errors/usage",
        default=True,
        description=(
            "Automatically share addon activity and any encountered errors "
            "with developers to help improve the product"
        ),
        update=optin_update
    )
    verbose_logs: bpy.props.BoolProperty(
        name="Verbose logging to console",
        default=True,
        description=(
            "Print out more verbose errors to the console, useful for "
            "troubleshooting issues"
        ),
        update=verbose_update
    )
    use_micro_displacements: bpy.props.BoolProperty(
        name="Use micro-displacements (if available, enables experimental)",
        default=False,
        description=(
            "Enable micro-displacements using adaptive subdivision.\n"
            "Note! This will enable Blender's experimental mode if not already active"
        )
    )
    show_updater_prefs: bpy.props.BoolProperty(
        name="Show/hide updater preferences",
        default=True,
        description="Show/hide updater-related preferences"
    )
    auto_check_update: bpy.props.BoolProperty(
        name="Auto-check for update (daily)",
        default=True,
        description=("Check for an addon update once per day,\n"
                     "only runs if the addon is in use.")
    )
    asset_browser_library_name: bpy.props.StringProperty(
        name="Library Name",
        default="Polligon Library",
        description=("Name of the library in Blender's Asset Browser")
    )
    asset_browser_mode: bpy.props.EnumProperty(
        name="Synchronization Mode",
        default="Disabled",
        items=[("Disabled", "Disabled", "No automatic synchronization"),
               ("On Download", "On Download", "Synchronize after download"),
               ("Automatic", "Automatic", "Synchronize all local assets on startup")],
        description=("Depending on this mode P4B will automatically\nsynchronize local assets with Blender's Asset Browser.")
    )
    any_owned_brushes: bpy.props.EnumProperty(
        name="Any Owned Brushes",
        default="undecided",
        items=[("undecided", "Undecided", "Still fetching pruchased assets info"),
               ("no_brushes", "No Brushes", "User owns no Brush assets"),
               ("owned_brushes", "Owned Brushes", "User owns Brush assets")],
        options={'HIDDEN'},
        description="Depending on this value, brush related setings will be hidden.")

    @reporting.handle_draw()
    def draw(self, context):
        f_BuildSettings(self, context)


def register():
    bpy.utils.register_class(PoliigonPreferences)
    optin_update(None, bpy.context)


def unregister():
    bpy.utils.unregister_class(PoliigonPreferences)
