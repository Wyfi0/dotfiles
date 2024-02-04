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

from datetime import datetime
from math import ceil
from typing import Dict, List, Optional, Tuple
import os
import platform
import re
import threading
import time

from bpy.types import Panel
import bpy

from . import reporting
from .toolbox import (cTB,
                      DisplayError,
                      Notification,
                      notification_signal_view,
                      f_login_with_website_handler,
                      ERR_LOGIN_TIMEOUT)
from .utils import *

THUMB_SIZE_FACTOR = {"Tiny": 0.5,
                     "Small": 0.75,
                     "Medium": 1.0,
                     "Large": 1.5,
                     "Huge": 2.0}


def safe_size_apply(op_ref: bpy.types.OperatorProperties,
                    size_value: str,
                    asset_name: str) -> None:
    """Applies a size value to operator draw with a safe fallback.

    If we try to apply a size which is not recognized as local, it will fail
    and disrupt further drawing. This function mitigates this problem.
    """
    try:
        op_ref.vSize = size_value
    except TypeError as e:
        # Since this is a UI draw issue, there will be multiple of these
        # these reports, but we have user-level debouncing for a max number
        # per message type.
        msg = f"Failed to assign {size_value} size for {asset_name}: {e}"
        print(msg)
        # TODO(SOFT-1303): Include in refactor to asset index, disabled
        # overreporting for now.
        # reporting.capture_message("failed_size_op_set", msg, "error")


def f_BuildUI(vUI, vContext):
    """Primary draw function used to build the main panel."""
    dbg = 0
    cTB.print_separator(dbg, "f_BuildUI")

    cTB._api._mp_relevant = True  # flag in request's meta data for Mixpanel

    if cTB.check_if_working():
        bpy.context.window.cursor_set("WAIT")
    elif cTB.vWasWorking:
        # No longer working, reset cursor. It is important that this is the
        # only place that the vWasWorking var is reset.
        cTB.vWasWorking = False
        bpy.context.window.cursor_set("DEFAULT")

    # ...............................................................................................

    cTB.vUI = vUI
    cTB.vContext = vContext

    if len(cTB.imported_assets.keys()) == 0:
        cTB.f_GetSceneAssets()

    cTB.vBtns = []

    for vA in bpy.context.screen.areas:
        if vA.type == "VIEW_3D":
            for vR in vA.regions:
                if vR.type == "UI":
                    panel_padding = 15 * cTB.get_ui_scale()  # Left padding.
                    sidebar_width = 15 * cTB.get_ui_scale()  # Tabname width.

                    # Mac blender 3.x up seems to be reported wider than
                    # reality; it does not seem affected by UI scale or HDPI.
                    ex_pad = "mac" in platform.platform()
                    ex_pad = ex_pad or "darwin" in platform.platform()
                    ex_pad = ex_pad and bpy.app.version >= (3, 0)
                    if ex_pad:
                        sidebar_width += 17 * cTB.get_ui_scale()
                    vWidth = vR.width - panel_padding - sidebar_width
                    if vWidth < 1:
                        # To avoid div by zero errors below
                        vWidth = 1
                    if vWidth != cTB.vWidth:
                        cTB.vWidth = vWidth
                        cTB.check_dpi()

    vSpc = 1.0 / cTB.vWidth

    vProps = bpy.context.window_manager.poliigon_props

    cTB.vSearch["poliigon"] = vProps.search_poliigon
    cTB.vSearch["my_assets"] = vProps.search_my_assets
    cTB.vSearch["imported"] = vProps.search_imported

    vArea = cTB.vSettings["area"]

    if cTB.vSearch[vArea] != cTB.vLastSearch[vArea]:
        cTB.vPage[vArea] = 0
        cTB.vPages[vArea] = 0

        cTB.vInterrupt = time.time()
        cTB.f_GetAssets()

        cTB.vLastSearch[vArea] = cTB.vSearch[vArea]

    vLayout = vUI.layout
    vLayout.alignment = "CENTER"

    vBaseRow = vLayout.row()

    cTB.vBase = vBaseRow.column()

    # NOTIFY ..................................................................

    cTB.interval_check_update()

    cTB.f_add_survey_notifcation_once()

    f_NotificationBanner(cTB.notifications, cTB.vBase)
    notice_ids = [ntc.notification_id for ntc in cTB.notifications]
    if "RESTART_POST_UPDATE" in notice_ids:
        msg = ("Updated addon files detected, please restart Blender to "
               "complete the installation")
        cTB.f_Label(
            cTB.vWidth,
            msg,
            cTB.vBase,
            vIcon="ERROR",
        )
        return

    # LOGIN ...................................................................

    if not cTB.is_logged_in():
        f_BuildLogin(cTB)
        return

    # LIBRARY .................................................................

    if not f_Ex(cTB.vSettings["library"]) or cTB.vWorking["startup"]:
        f_BuildLibrary(cTB)

        return

    # Section + asset balance .................................................

    split_fac = 1.0 - (70.0 / cTB.vWidth * cTB.get_ui_scale())
    vSplit = cTB.vBase.split(factor=split_fac)

    area_title = " ".join(
        [vS.capitalize() for vS in cTB.vSettings["area"].split("_")]
    )
    if area_title == "Poliigon":
        area_title = "Online"
    if cTB.vSettings["show_settings"]:
        area_title = "Settings"
    elif cTB.vSettings["show_user"]:
        area_title = "My Account"

    vSplit.label(text=area_title)

    # Asset balance

    balance_icon = cTB.vIcons["ICON_asset_balance"].icon_id
    if cTB.vUser["plan_paused"]:
        if cTB.vUser["credits_od"] > 0:
            credits = str(cTB.vUser["credits_od"])
        else:
            credits = str(cTB.vUser["credits"])
            balance_icon = cTB.vIcons["ICON_subscription_paused"].icon_id
    else:
        credits = str(cTB.vUser["credits"] + cTB.vUser["credits_od"])

    vOpCredits = vSplit.operator(
        "poliigon.poliigon_setting",
        text=credits,
        icon_value=balance_icon  # TODO: use new asset icon
    )
    vOpCredits.vTooltip = (
        "Your asset balance shows how many assets you can\n"
        "purchase. Free assets and downloading assets you\n"
        "already own doesn’t affect your balance")
    vOpCredits.vMode = "show_user"

    cTB.vBase.separator()

    # AREAS ...................................................................

    cTB.print_debug(dbg, "f_BuildUI", "f_BuildAreas")
    f_BuildAreas(cTB)

    # USER ....................................................................

    if cTB.vSettings["show_user"]:
        f_BuildUser(cTB)
        return

    # SEARCH ..................................................................

    vRow = cTB.vBase.row()

    vRow1 = vRow.row(align=True)

    # NEED SEPARATE PROPS FOR SPECIFIC DESCRIPTIONS

    vRow1.prop(vProps, f"search_{cTB.vSettings['area']}", icon="VIEWZOOM")

    vShowX = 0
    if vArea == 'poliigon' and len(vProps.search_poliigon):
        vShowX = 1
    elif vArea == 'my_assets' and len(vProps.search_my_assets):
        vShowX = 1
    elif vArea == 'imported' and len(vProps.search_imported):
        vShowX = 1

    if vShowX:
        vOp = vRow1.operator(
            "poliigon.poliigon_setting",
            text="",
            icon="X",
        )
        vOp.vTooltip = "Clear Search"
        vOp.vMode = f"clear_search_{cTB.vSettings['area']}"

    vRow1.separator()
    vOp = vRow1.operator(
        "poliigon.refresh_data",
        text="",
        icon="FILE_REFRESH"
    )

    # ASSET LIST ..............................................................

    cTB.vActiveCat = cTB.vSettings["category"][vArea]

    cTB.vAssetType = cTB.vActiveCat[0]

    # CATEGORY ................................................................

    cTB.print_debug(dbg, "f_BuildUI", "f_BuildCategories")
    f_BuildCategories(cTB)

    # ASSETS ..................................................................

    cTB.print_debug(dbg, "f_BuildUI", "f_BuildAssets")
    f_BuildAssets(cTB)


# .............................................................................
# Draw utilities
# .............................................................................

def _draw_welcome_or_error(layout: bpy.types.UILayout) -> None:
    if cTB.user_invalidated() and cTB.vWorking["login"] == 0:
        layout.separator()

        if cTB.vLoginError == ERR_LOGIN_TIMEOUT:
            cTB.f_Label(
                cTB.vWidth,
                cTB.vLoginError,
                layout,
                vIcon="ERROR",
            )
        else:
            cTB.f_Label(
                cTB.vWidth,
                "Warning : You have been logged out as this account was signed in on another device.",
                layout,
                vIcon="ERROR",
            )

    else:
        cTB.f_Label(
            cTB.vWidth,
            "Welcome to the Poliigon Addon!",
            layout,
        )

    layout.separator()


def _draw_share_addon_errors(layout: bpy.types.UILayout,
                             enabled: bool = True) -> None:
    # Show terms of service, optin/out.
    opt_row = layout.row()
    opt_row.alignment = "LEFT"
    opt_row.enabled = enabled
    prefs = bpy.context.preferences.addons.get(__package__, None)
    opt_row.prop(prefs.preferences, "reporting_opt_in", text="")
    twidth = cTB.vWidth - 42 * cTB.get_ui_scale()
    cTB.f_Label(twidth, "Share addon errors / usage", opt_row)


def _draw_switch_email_login(col: bpy.types.UILayout,
                             enabled: bool = True) -> None:
    row_login_email = col.row()
    row_login_email.enabled = enabled
    op_login_email = row_login_email.operator("poliigon.poliigon_user",
                                              text="Login via email",
                                              emboss=False)
    op_login_email.vMode = "login_switch_to_email"
    op_login_email.vTooltip = "Login via email"


def _draw_browser_login(col: bpy.types.UILayout) -> None:
    if bpy.app.timers.is_registered(f_login_with_website_handler):
        _draw_share_addon_errors(col, enabled=False)

        row_buttons = col.row(align=True)
        row_buttons.scale_y = 1.25

        col1 = row_buttons.column(align=True)
        op_login_website = col1.operator("poliigon.poliigon_user",
                                         text="Opening browser...",
                                         depress=True)
        op_login_website.vMode = "none"
        op_login_website.vTooltip = "Complete login via opened webpage"
        col1.enabled = False

        col2 = row_buttons.column(align=True)
        op_login_cancel = col2.operator("poliigon.poliigon_user",
                                        text="",
                                        icon="X")
        op_login_cancel.vMode = "login_cancel"
        op_login_cancel.vTooltip = "Cancel Log In"

        col.separator()

        _draw_switch_email_login(col, enabled=False)
    else:
        _draw_share_addon_errors(col)

        row_button = col.row()
        row_button.scale_y = 1.25

        op_login_website = row_button.operator("poliigon.poliigon_user",
                                               text="Login via Browser")
        op_login_website.vMode = "login_with_website"
        op_login_website.vTooltip = "Login via Browser"

        col.separator()

        _draw_switch_email_login(col)


def _draw_email_login(col: bpy.types.UILayout) -> None:
    vProps = bpy.context.window_manager.poliigon_props

    col.label(text="Email")

    row = col.row(align=True)
    row.prop(vProps, "vEmail")

    col_x = row.column(align=True)
    op = col_x.operator("poliigon.poliigon_setting",
                        text="",
                        icon="X")
    op.vTooltip = "Clear Email"
    op.vMode = "clear_email"

    error_credentials = False
    error_login = cTB.vLoginError and cTB.vLoginError != ERR_LOGIN_TIMEOUT
    if error_login and "@" not in vProps.vEmail:
        error_credentials = True

        col.separator()
        cTB.f_Label(cTB.vWidth - 40 * cTB.get_ui_scale(),
                    "Email format is invalid e.g. john@example.org",
                    col,
                    vIcon="ERROR")
    col.separator()

    col.label(text="Password")

    row = col.row(align=True)

    if cTB.vSettings["show_pass"]:
        row.prop(vProps, "vPassShow")
        vPass = vProps.vPassShow

    else:
        row.prop(vProps, "vPassHide")
        vPass = vProps.vPassHide

    col_x = row.column(align=True)

    op = col_x.operator("poliigon.poliigon_setting",
                        text="",
                        icon="X")
    op.vTooltip = "Clear Password"
    op.vMode = "clear_pass"

    if error_login and len(vPass) < 6:
        error_credentials = True

        col.separator()
        cTB.f_Label(cTB.vWidth - 40 * cTB.get_ui_scale(),
                    "Password should be at least 6 characters.",
                    col,
                    vIcon="ERROR")
    col.separator()

    _draw_share_addon_errors(col)

    enable_login_button = len(vProps.vEmail) > 0 and len(vPass) > 0

    row = col.row()
    row.scale_y = 1.25

    if cTB.vWorking["login"]:
        op_login = row.operator("poliigon.poliigon_setting",
                                text="Logging In...",
                                depress=enable_login_button)
        op_login.vMode = "none"
        op_login.vTooltip = "Logging In..."
        row.enabled = False
    else:
        op_login = row.operator("poliigon.poliigon_user",
                                text="Login via email")
        op_login.vMode = "login"
        op_login.vTooltip = "Login via email"

        row.enabled = enable_login_button

    if cTB.vLoginError == cTB.ERR_CREDS_FORMAT:
        # Will draw above with more specific messages if condition true, like
        # invalid email format or password length.
        pass
    elif error_login and not error_credentials:
        col.separator()

        cTB.f_Label(
            cTB.vWidth - 40 * cTB.get_ui_scale(),
            cTB.vLoginError,
            col,
            vIcon="ERROR",
        )

    col.separator()

    op_forgot = col.operator("poliigon.poliigon_link",
                             text="Forgot Password?",
                             emboss=False)
    op_forgot.vMode = "forgot"
    op_forgot.vTooltip = "Reset your Poliigon password"

    op_login_website = col.operator("poliigon.poliigon_user",
                                    text="Login via Browser",
                                    emboss=False)
    op_login_website.vMode = "login_switch_to_browser"
    op_login_website.vTooltip = "Login via Browser"


def _draw_login(layout: bpy.types.UILayout) -> None:
    spc = 1.0 / cTB.vWidth

    box = layout.box()
    row = box.row()
    row.separator(factor=spc)
    col = row.column()
    row.separator(factor=spc)

    twidth = cTB.vWidth - 42 * cTB.get_ui_scale()
    cTB.f_Label(twidth, "Login", col)
    col.separator()

    if cTB.login_via_browser:
        _draw_browser_login(col)

    else:
        _draw_email_login(col)


def _draw_signup(layout: bpy.types.UILayout) -> None:
    cTB.f_Label(
        cTB.vWidth,
        "Don't have an account?",
        layout,
    )
    op_signup = layout.operator("poliigon.poliigon_link",
                                text="Sign Up")
    op_signup.vMode = "signup"
    op_signup.vTooltip = "Create a Poliigon account"


def _draw_legal(layout: bpy.types.UILayout) -> None:
    row = layout.row()
    col = row.column(align=True)

    op_terms = col.operator("poliigon.poliigon_link",
                            text="Terms & Conditions",
                            emboss=False)
    op_terms.vTooltip = "View the terms and conditions page"
    op_terms.vMode = "terms"

    op_privacy = col.operator("poliigon.poliigon_link",
                              text="Privacy Policy",
                              emboss=False)
    op_privacy.vTooltip = "View the Privacy Policy "
    op_privacy.vMode = "privacy"


# @timer
def f_BuildLogin(cTB):
    dbg = 0
    cTB.print_separator(dbg, "f_BuildLogin")

    if cTB.vLoginError:
        cTB.vWorking["login"] = 0

    _draw_welcome_or_error(cTB.vBase)
    _draw_login(cTB.vBase)
    cTB.vBase.separator()
    _draw_signup(cTB.vBase)
    cTB.vBase.separator()
    _draw_legal(cTB.vBase)


# @timer
def f_BuildLibrary(cTB):
    dbg = 0
    cTB.print_separator(dbg, "f_BuildLibrary")
    vSpc = 1.0 / cTB.vWidth

    # ...............................................................................................

    cTB.f_Label(
        cTB.vWidth,
        "Welcome to the Poliigon Addon!",
        cTB.vBase,
    )

    cTB.vBase.separator()

    cTB.f_Label(
        cTB.vWidth,
        "Select where you will store Poliigon assets.",
        cTB.vBase,
    )

    cTB.vBase.separator()

    # ...............................................................................................

    vBRow = cTB.vBase.box().row()
    vBRow.separator(factor=vSpc)
    vCol = vBRow.column()
    vBRow.separator(factor=vSpc)

    # ...............................................................................................

    vCol.label(text="Library Location")

    vLbl = cTB.vSettings["set_library"]
    if vLbl == "":
        vLbl = "Select Location"

    vOp = vCol.operator(
        "poliigon.poliigon_library",
        icon="FILE_FOLDER",
        text=vLbl,
    )
    vOp.vMode = "set_library"
    vOp.directory = cTB.vSettings["set_library"]
    vOp.vTooltip = "Select Location"

    vCol.separator()
    vConformRow = vCol.row()
    vConformRow.scale_y = 1.5

    if cTB.vWorking["startup"]:
        vOp = vConformRow.operator(
            "poliigon.poliigon_setting", text="Confirming...", depress=1
        )
        vOp.vMode = "none"
        vOp.vTooltip = "Confirming Library Location..."
        vConformRow.enabled = False
    else:
        vOp = vConformRow.operator("poliigon.poliigon_setting", text="Confirm")
        vOp.vMode = "set_library"
        vOp.vTooltip = "Confirm Library location"

    vCol.separator()

    # ...............................................................................................

    cTB.f_Label(
        cTB.vWidth - 30 * cTB.get_ui_scale(),
        "You can change this and add more directories in the settings at any time.",
        vCol,
    )

    vCol.separator()

    # ...............................................................................................

    cTB.vBase.separator()

    cTB.vWorking["startup"] = False


# @timer
def f_BuildUser(cTB):
    dbg = 0
    cTB.print_separator(dbg, "f_BuildUser")

    vSpc = 1.0 / cTB.vWidth

    # YOUR CREDITS ............................................................

    vBox = cTB.vBase.box()

    vOp = vBox.operator(
        "poliigon.poliigon_setting",
        text="Asset Balance    ",
        icon="DISCLOSURE_TRI_DOWN"
        if cTB.vSettings["show_credits"]
        else "DISCLOSURE_TRI_RIGHT",
        emboss=0,
    )
    vOp.vMode = "show_credits"
    if cTB.vSettings["show_credits"]:
        vOp.vTooltip = "Hide Your Asset Balance"
    else:
        vOp.vTooltip = "Show Your Asset Balance"

    if cTB.vSettings["show_credits"]:
        vBRow = vBox.row()
        vBRow.separator(factor=vSpc)
        vCol = vBRow.column()
        vBRow.separator(factor=vSpc)

        asset_balance = cTB.vUser["credits"] + cTB.vUser["credits_od"]

        vCol.label(text=str(asset_balance))

        # View how many credits to expect in certian number of days.
        if cTB.vUser["plan_name"]:
            next_credits = cTB.vUser["plan_next_credits"]
            amount = cTB.vUser["plan_credit"]
            try:
                dt = datetime.strptime(next_credits, "%Y-%m-%d")
            except TypeError:
                dt = None
            except ValueError:
                dt = None

            amount = cTB.vUser["plan_credit"]
            now = datetime.now()

            # Compute diffs only on overall day.
            if dt is not None:
                now = now.replace(hour=0, minute=0, second=0, microsecond=0)
                dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

                diff = dt - now
                in_days = diff.days

                pause = " (paused)" if cTB.vUser["plan_paused"] else ""

                if in_days >= 0:
                    cTB.f_Label(
                        cTB.vWidth - 40 * cTB.get_ui_scale(),
                        f"+{amount} in {in_days} days{pause}",
                        vCol)

        vCol.separator()

    cTB.vBase.separator()

    # YOUR PLAN ...............................................................

    vBox = cTB.vBase.box()

    vOp = vBox.operator(
        "poliigon.poliigon_setting",
        text="Your Plan     ",
        icon="DISCLOSURE_TRI_DOWN"
        if cTB.vSettings["show_plan"]
        else "DISCLOSURE_TRI_RIGHT",
        emboss=0,
    )
    vOp.vMode = "show_plan"
    if cTB.vSettings["show_plan"]:
        vOp.vTooltip = "Hide Your Plan Details"
    else:
        vOp.vTooltip = "Show Your Plan Details"

    if cTB.vSettings["show_plan"]:
        vBRow = vBox.row()
        vBRow.separator(factor=vSpc)
        vCol = vBRow.column()
        vBRow.separator(factor=vSpc)

        vCol.separator()

        if not cTB.vUser["plan_name"]:
            cTB.f_Label(
                cTB.vWidth - 20 * cTB.get_ui_scale(),
                "Subscribe to a Poliigon Plan and start downloading assets.",
                vCol,
            )

            vCol.separator()

            vOp = vCol.operator("poliigon.poliigon_link", text="Subscribe Now")
            vOp.vMode = "subscribe"
            vOp.vTooltip = "Start a Poliigon subscription"

        else:
            plan_name = cTB.vUser["plan_name"]
            pause = " (PAUSED)" if cTB.vUser["plan_paused"] else ""

            cTB.f_Label(
                cTB.vWidth - 40 * cTB.get_ui_scale(),
                f"{plan_name}{pause}",
                vCol)

            if cTB.vUser["plan_paused"]:
                pause_date = cTB.vUser["plan_paused_at"].split(" ")[0]
                pause_until = cTB.vUser["plan_paused_until"].split(" ")[0]
                label = f"Subscription paused on {pause_date} until {pause_until}"
                cTB.f_Label(
                    cTB.vWidth - 40 * cTB.get_ui_scale(),
                    label,
                    vCol)
            else:
                next_renew = cTB.vUser["plan_next_renew"]
                cTB.f_Label(
                    cTB.vWidth - 40 * cTB.get_ui_scale(),
                    f"Renews on {next_renew}",
                    vCol)

            vCol.separator()

            credits = cTB.vUser["plan_credit"]
            cTB.f_Label(
                cTB.vWidth - 40 * cTB.get_ui_scale(),
                f"Assets: {credits} per month",
                vCol)

            # TODO: cTB.f_Label(f"Price: {symb}{amt}{curr} per {period}")

            vCol.separator()

            vOp = vCol.operator("poliigon.poliigon_link", text="Upgrade Plan")
            vOp.vMode = "credits"
            vOp.vTooltip = "Change your Poliigon Plan Online"

        vCol.separator()

        cTB.vBase.separator()

    # YOUR PLAN ...............................................................

    cTB.vBase.separator()
    box = cTB.vBase.box()

    ops = box.operator(
        "poliigon.poliigon_setting",
        text="Addon feedback     ",
        icon="DISCLOSURE_TRI_DOWN"
        if cTB.vSettings["show_feedback"]
        else "DISCLOSURE_TRI_RIGHT",
        emboss=0,
    )
    ops.vMode = "show_feedback"
    if cTB.vSettings["show_feedback"]:
        ops.vTooltip = "Hide Feedback Details"
    else:
        ops.vTooltip = "Show Feedback Details"

    if cTB.vSettings["show_feedback"]:
        lbl_width = cTB.vWidth - 20 * cTB.get_ui_scale()

        msg = "Tell us how satisfied you are with this addon"
        cTB.f_Label(lbl_width, msg, box, vAddPadding=False)
        ops = box.operator("poliigon.poliigon_link", text="Feedback survey")
        ops.vTooltip = msg
        ops.vMode = "survey"

        # Create small spacer
        _ = box.row()

        msg = "Have any suggestions? Share and upvote ideas below"
        cTB.f_Label(lbl_width, msg, box, vAddPadding=False)
        ops = box.operator("poliigon.poliigon_link", text="Share suggestions")
        ops.vTooltip = "Share and upvote ideas in the Blender Addon board"
        ops.vMode = "suggestions"

    cTB.vBase.separator()
    vOp = cTB.vBase.operator("poliigon.poliigon_user", text="Log Out")
    vOp.vMode = "logout"
    vOp.vTooltip = "Log Out of Poliigon"


# @timer
def f_BuildAreas(cTB):
    dbg = 0
    cTB.print_separator(dbg, "f_BuildAreas")
    cTB.initial_view_screen()

    vRow = cTB.vBase.row(align=True)
    vRow.scale_x = 1.1
    vRow.scale_y = 1.1

    vDep = not cTB.vSettings["show_user"] and not cTB.vSettings["show_settings"]

    vCol = vRow.column(align=True)
    vDep1 = cTB.vSettings["area"] == "poliigon"
    vOp = vCol.operator(
        "poliigon.poliigon_setting",
        text="",
        icon_value=cTB.vIcons["ICON_poliigon"].icon_id,
        depress=vDep1 and vDep,
    )
    vOp.vMode = "area_poliigon"
    vOp.vTooltip = "Show Poliigon Assets"

    vCol = vRow.column(align=True)
    vDep1 = cTB.vSettings["area"] == "my_assets"
    vOp = vCol.operator(
        "poliigon.poliigon_setting",
        text="",
        icon_value=cTB.vIcons["ICON_myassets"].icon_id,
        depress=vDep1 and vDep,
    )
    vOp.vMode = "area_my_assets"
    vOp.vTooltip = "Show My Assets"

    vCol = vRow.column(align=True)
    vDep1 = cTB.vSettings["area"] == "imported"
    vOp = vCol.operator(
        "poliigon.poliigon_setting",
        text="",
        icon="OUTLINER_OB_GROUP_INSTANCE",
        depress=vDep1 and vDep,
    )
    vOp.vMode = "area_imported"
    vOp.vTooltip = "Show Imported Assets"

    vOp = vRow.operator(
        "poliigon.poliigon_setting",
        text="",
        icon="COMMUNITY",
        depress=cTB.vSettings["show_user"],
    )
    vOp.vMode = "my_account"
    vOp.vTooltip = "Show Your Account Details"

    vSRow = vRow.row(align=False)
    vSRow.alignment = "RIGHT"
    vOp = vSRow.operator(
        "poliigon.open_preferences",
        text="",
        icon="PREFERENCES",
    ).set_focus = "all"

    cTB.vBase.separator()


# @timer
def f_BuildCategories(cTB):
    dbg = 0
    cTB.print_separator(dbg, "f_BuildCategories")

    vSpc = 1.0 / cTB.vWidth

    vCats = []
    vCategories = []
    vSubs = []
    if cTB.vAssetType != "All Assets":
        for vType in cTB.vCategories["poliigon"].keys():
            if cTB.vAssetType in ["All Assets", vType]:
                vCategories += cTB.vCategories["poliigon"][vType].keys()
        vCategories = sorted(list(set(vCategories)))

        if len(vCategories):
            vCategory = ""
            vCats = []
            for i in range(1, len(cTB.vActiveCat)):
                vCategory += "/" + cTB.vActiveCat[i]
                vCats.append(vCategory)

            vSubs = [
                vC.split("/")[-1]
                for vC in vCategories
                if vC.startswith(vCategory) and vC != vCategory
            ]
            if len(vSubs):
                vCats.append("sub")

    gCatsCol = cTB.vBase.column()

    width_factor = len(vCats) + 1
    if cTB.vWidth >= max(width_factor, 2) * 160 * cTB.get_ui_scale():
        vRow = gCatsCol.row()
    else:
        vRow = gCatsCol

    vRow1 = vRow.row(align=True)

    vTypes = ["All Assets", "Textures", "Models", "HDRIs"]

    vOp = vRow1.operator(
        "poliigon.poliigon_category", text=cTB.vAssetType, icon="TRIA_DOWN"
    )
    vOp.vData = "0@" + "@".join(vTypes)

    if vCats:
        for i in range(len(vCats)):
            vCat = vCats[i]

            vRow1 = vRow.row(align=True)

            if i == 0:
                vSCats = [
                    vC.split("/")[-1]
                    for vC in vCategories
                    if len(vC.split("/")) == 2
                ]
            elif vCat == "sub":
                vSCats = vSubs
            else:
                vPCat = "/".join(vCat.split("/")[:-1])
                vSCats = [
                    vC.split("/")[-1]
                    for vC in vCategories
                    if vC.startswith(vPCat) and vC != vPCat
                ]

            vSCats = sorted(list(set(vSCats)))

            vLbl = vCat.split("/")[-1]
            if vCat == "sub":
                vLbl = "All " + cTB.vActiveCat[-1]

            vSCats.insert(0, "All " + cTB.vActiveCat[i])
            vData = str(i + 1) + "@" + "@".join(vSCats)

            vOp = vRow1.operator(
                "poliigon.poliigon_category", text=vLbl, icon="TRIA_DOWN"
            )
            vOp.vData = vData

    gCatsCol.separator()


def determine_downloaded(asset_data: Dict) -> bool:
    """Returns True if the asset should be considered local with current settings."""

    asset_name = asset_data["name"]
    asset_type = asset_data["type"]
    asset_files = asset_data["files"]

    with cTB.lock_assets:
        assets_local = cTB.vAssets["local"]

        if asset_type not in assets_local.keys():
            return False

        if asset_name not in assets_local[asset_type].keys():
            return False

    is_downloaded = False
    prefer_blend = cTB.vSettings["download_prefer_blend"]
    if asset_type == "Models" and prefer_blend:
        # Force display needing blend download, if prefer blend
        # active and e.g. only FBX local.
        for path_asset in asset_files:
            is_blend = path_asset.endswith(".blend")
            is_asset_browser = "_LIB.blend" in path_asset
            if is_blend and not is_asset_browser:
                is_downloaded = True
                break
    elif asset_type == "Models" and not prefer_blend:
        # Force display needing FBX download, if prefer blend
        # active and e.g. only blend local.
        for path_asset in asset_files:
            is_fbx = path_asset.endswith(".fbx")
            if is_fbx:
                is_downloaded = True
                break
    elif asset_type == "HDRIs":
        # Force button to show "download", if the preferred size(s)
        # are not available locally
        exr_is_local = False
        for path_asset in asset_files:
            filename = os.path.basename(path_asset)
            if filename.endswith(".exr"):
                exr_is_local |= cTB.vSettings["hdri"] in filename
        if cTB.vSettings["hdri_use_jpg_bg"]:
            jpg_is_local = False
            for path_asset in asset_files:
                filename = os.path.basename(path_asset)
                if filename.endswith(".jpg") and "_JPG" in filename:
                    jpg_is_local |= cTB.vSettings["hdrib"] in filename
            is_downloaded = exr_is_local and jpg_is_local
        else:
            is_downloaded = exr_is_local
    else:
        is_downloaded = True

    return is_downloaded


def build_assets_no_assets(area: str, category: str) -> None:
    box_not_found = cTB.vBase.box()

    label = f"No Poliigon {category} found in Library"
    if cTB.vSearch[area] != "":
        label = (
            "No results found."
            " Please try changing your filter or search term."
        )
    elif area == "imported":
        label = f"No Poliigon {category} found in the Scene"
    elif area == "poliigon":
        label = f"No Poliigon {category} found Online"

    width = cTB.vWidth - 20 * cTB.get_ui_scale()
    cTB.f_Label(width, label, box_not_found, vAddPadding=True)

    return box_not_found


def build_assets_prepare_grid(thumb_size_factor: float,
                              sorted_assets: List[Dict]
                              ) -> Tuple[bpy.types.UILayout, float, int]:
    thumb_width = 170
    thumb_width = ceil(thumb_width * thumb_size_factor)
    thumb_width *= cTB.get_ui_scale()

    num_columns = int(cTB.vWidth / thumb_width)
    if num_columns == 0:
        num_columns = 1
    if num_columns > len(sorted_assets):
        num_columns = len(sorted_assets)

    padding = (cTB.vWidth - (num_columns * thumb_width)) / 2
    if padding < 1.0 and num_columns > 1:
        num_columns -= 1
        padding = (cTB.vWidth - (num_columns * thumb_width)) / 2

    if padding < 1.0 or thumb_width + 1 > cTB.vWidth:
        # Panel is narrower than a single preview width, single col.
        grid = cTB.vBase.grid_flow(
            row_major=True, columns=num_columns,
            even_columns=True, even_rows=True, align=False
        )

    else:
        # Typical case, fit rows and columns.
        factor = padding / cTB.vWidth
        split_left = cTB.vBase.split(factor=factor)

        split_left.separator()

        factor = 1.0 - factor
        split_right = split_left.split(factor=factor)

        grid = split_right.grid_flow(
            row_major=True, columns=num_columns,
            even_columns=True, even_rows=True, align=False
        )

        split_right.separator()

    return grid, thumb_width, num_columns


def get_active_brush() -> str:
    brush = None

    try:
        sculpt_brush = bpy.context.tool_settings.sculpt.brush
        if sculpt_brush.name == "Poliigon":
            brush = sculpt_brush.texture.image.poliigon.split(";")[1]
    except:
        pass

    return brush


def get_local_sizes(asset_data: Dict) -> List[str]:
    sizes_local = []

    asset_type = asset_data["type"]
    asset_name = asset_data["name"]

    with cTB.lock_assets:
        assets_local = cTB.vAssets["local"]

        if asset_type in assets_local.keys():
            assets_local_by_type = assets_local[asset_type]
            if asset_name in assets_local_by_type.keys():
                asset_data_local = assets_local_by_type[asset_name]

                for key in ["files", "lods"]:
                    asset_data[key] = asset_data_local[key]

                sizes_local = asset_data_local["sizes"]

    return sizes_local


def determine_default_size(asset_data: Dict,
                           asset_sizes_local: List[str],
                           is_downloaded: bool
                           ) -> str:
    size_default = ""

    asset_type = asset_data["type"]

    sizes_check = asset_data["sizes"]
    if len(asset_sizes_local):
        sizes_check = asset_sizes_local

    if len(sizes_check):
        if asset_type == "Textures":
            size_default = cTB.f_GetClosestSize(sizes_check,
                                                cTB.vSettings["res"])
        elif asset_type == "Models":
            size_default = cTB.f_GetClosestSize(sizes_check,
                                                cTB.vSettings["mres"])
        elif asset_type == "HDRIs":
            if is_downloaded:
                size_default = cTB.f_GetClosestSize(sizes_check,
                                                    cTB.vSettings["hdri"])
            else:
                size_default = cTB.vSettings["hdri"]
        elif asset_type == "Brushes":
            size_default = cTB.f_GetClosestSize(sizes_check,
                                                cTB.vSettings["brush"])
    return size_default


def determine_in_scene_sizes(asset_data: Dict,
                             size_default: str
                             ) -> Tuple[List[str], str]:
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]

    sizes_in_scene = []
    if asset_type not in cTB.imported_assets.keys():
        return sizes_in_scene, size_default

    if asset_name not in cTB.imported_assets[asset_type].keys():
        return sizes_in_scene, size_default

    objlist = cTB.imported_assets[asset_type][asset_name]
    idxs_remove_object = []
    for idx_obj, obj in enumerate(objlist):
        try:
            sizes_in_scene.append(cTB.f_GetSize(obj.name))
        except ReferenceError:
            # Object was removed, so pop from the list (see below).
            idxs_remove_object.append(idx_obj)
        except AttributeError as err:
            print("Failed to vInScene.append")
            print(err)
            # But continue to avoid complete UI breakage.
    for idx_obj in reversed(idxs_remove_object):
        objlist.pop(idx_obj)

    if sizes_in_scene and size_default not in sizes_in_scene and sizes_in_scene[0]:
        size_default = sizes_in_scene[0]

    return sizes_in_scene, size_default


def draw_thumbnail(asset_data: Dict,
                   thumb_size_factor: float,
                   layout_box: bpy.types.UILayout) -> None:
    asset_name = asset_data["name"]
    thumb_scale = cTB.vSettings["preview_size"] * thumb_size_factor

    with cTB.lock_previews:
        if asset_name == "dummy":
            layout_box.template_icon(
                icon_value=cTB.vIcons["GET_preview"].icon_id,
                scale=thumb_scale
            )
        elif asset_name in cTB.vPreviews.keys():
            layout_box.template_icon(
                icon_value=cTB.vPreviews[asset_name].icon_id,
                scale=thumb_scale
            )

            if asset_name in cTB.vPreviewsDownloading:
                cTB.vPreviewsDownloading.remove(asset_name)

        else:
            if asset_name in cTB.vPreviewsDownloading:
                layout_box.template_icon(
                    icon_value=cTB.vIcons["GET_preview"].icon_id,
                    scale=thumb_scale
                )

            else:
                layout_box.template_icon(
                    icon_value=cTB.vIcons["NO_preview"].icon_id,
                    scale=thumb_scale
                )


def draw_thumb_state_asset_dummy(layout_row: bpy.types.UILayout) -> None:
    op = layout_row.operator("poliigon.poliigon_setting", text="  ")
    op.vMode = "none"


def draw_thumb_state_asset_purchasing(layout_row: bpy.types.UILayout) -> None:
    op = layout_row.operator(
        "poliigon.poliigon_setting",
        text="Purchasing...",
        emboss=1,
        depress=1,
    )
    op.vMode = "none"
    op.vTooltip = "Purchasing..."


def draw_thumb_state_asset_downloading(layout_row: bpy.types.UILayout,
                                       asset_data: Dict,
                                       thumb_width: float
                                       ) -> None:
    asset_id = asset_data["id"]
    asset_name = asset_data["name"]

    with cTB.lock_download:
        download_data = cTB.vDownloadQueue[asset_id].copy()

    download_file = download_data.get("download_file", "")
    remaining_time = None
    if f_Ex(download_file):
        if download_data.get("download_size") is not None:
            download_size = download_data["download_size"]
            try:
                file_size = os.path.getsize(download_file)
                time_file = os.path.getctime(download_file)
            except BaseException as e:  # any exception
                # As the file existed two lines above and now it's gone,
                # we assume it is complete
                file_size = download_size
                # any time is good, will be multiplied by zero anyway
                time_file = time.time()
            if file_size > 0:
                download_time = time.time() - time_file
                remaining_time = (download_time / file_size) * (download_size - file_size)

                if remaining_time > 60 * 60:
                    gm_remaining_time = time.gmtime(remaining_time)
                    hours = int(time.strftime("%H", gm_remaining_time))
                    remaining_time = f"{hours}h+"
                elif remaining_time > 60:
                    gm_remaining_time = time.gmtime(remaining_time)
                    minutes = int(time.strftime("%M", gm_remaining_time))
                    remaining_time = f"{minutes}m+"
                elif remaining_time <= 60:
                    gm_remaining_time = time.gmtime(remaining_time)
                    seconds = int(time.strftime("%S", gm_remaining_time))
                    remaining_time = f"{seconds}s"

    progress = download_data.get("download_percent", 0.001)  # zero is not a good value for the row.split

    layout_row.label(text="", icon="IMPORT")

    col = layout_row.column()
    col_cancel = layout_row.column()
    # Display cancel button instead of time remaining.
    ops = col_cancel.operator("poliigon.cancel_download",
                              emboss=False, text="", icon="X")
    ops.asset_id = asset_id

    spacer = col.row()
    spacer.scale_y = 0.2
    spacer.label(text="")

    row_progress = col.row()
    row_progress.scale_y = 0.4

    split_progress = row_progress.split(factor=progress, align=True)
    pcent = round(progress * 100, 1)
    tooltip = f"Downloading ({pcent}%)\n{asset_name} @ {download_data['size']}..."

    op = split_progress.operator(
        "poliigon.poliigon_setting", text="", emboss=1, depress=1
    )
    op.vMode = "none"
    op.vTooltip = tooltip

    op = split_progress.operator(
        "poliigon.poliigon_setting", text="", emboss=1, depress=0
    )
    op.vMode = "none"
    op.vTooltip = tooltip

    layout_row.separator()


def draw_thumb_state_asset_downloading_quick_preview(
        layout_row: bpy.types.UILayout, asset_data: Dict) -> None:
    asset_name = asset_data["name"]

    downloaded_files = [
        path
        for path in cTB.vQuickPreviewQueue[asset_name]
        if f_Ex(path)
    ]
    progress = len(downloaded_files) / len(cTB.vQuickPreviewQueue[asset_name])

    layout_row.label(text="", icon="IMPORT")

    col = layout_row.column()

    spacer = col.row()
    spacer.scale_y = 0.2
    spacer.label(text="")

    row_progress = col.row()
    row_progress.scale_y = 0.4

    split_progress = row_progress.split(factor=progress / 10, align=True)

    op = split_progress.operator(
        "poliigon.poliigon_setting", text="", emboss=1, depress=1
    )
    op.vMode = "none"
    op.vTooltip = "Downloading..."

    op = split_progress.operator(
        "poliigon.poliigon_setting", text="", emboss=1, depress=0
    )
    op.vMode = "none"
    op.vTooltip = "Downloading..."

    layout_row.separator()

    if progress >= 9.9:
        del cTB.vQuickPreviewQueue[asset_name]
        cTB.vRedraw = 1


def draw_button_quick_preview(layout_row: bpy.types.UILayout,
                              asset_data: Dict,
                              is_backplate: bool,
                              is_selection: bool
                              ) -> None:
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]

    do_show = False
    if is_backplate and asset_data["preview"] != "":
        do_show = True
    elif len(asset_data["quick_preview"]):
        do_show = True

    if not do_show:
        return

    col_preview = layout_row.column(align=True)
    col_preview.enabled = is_selection or is_backplate

    op = col_preview.operator(
        "poliigon.poliigon_preview",
        text="",
        icon="HIDE_OFF",
        emboss=1,
    )
    op.vType = asset_type
    op.vAsset = asset_name
    if is_selection:
        op.vTooltip = f'Preview {asset_name} on Selected Object(s)'
    else:
        op.vTooltip = "Select an object to preview this texture"


def draw_checkmark_imported(layout_row: bpy.types.UILayout) -> None:
    col_checkmark = layout_row.column(align=True)
    col_checkmark.enabled = False
    icon_val = cTB.vIcons["ICON_acquired_check"].icon_id
    op = col_checkmark.operator(
        "poliigon.poliigon_setting",
        text="",
        icon_value=icon_val,
        depress=False,
        emboss=True
    )
    op.vTooltip = "Asset already acquired"


def draw_button_model_local(layout_row: bpy.types.UILayout,
                            asset_data: Dict,
                            error: DisplayError
                            ) -> None:
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]

    sizes_local = get_local_sizes(asset_data)
    size_desired = cTB.get_last_downloaded_size(asset_name,
                                                cTB.vSettings["mres"])
    size = cTB.f_GetClosestSize(sizes_local, size_desired)

    if error:
        icon = "ERROR"
        label = error.button_label
        lod = "NONE"
        tip = error.description
    else:
        lod, label, tip = get_model_op_details(asset_name, asset_type, size)
        if lod != "" and lod != "NONE":
            label = f"Import {size}, {lod}"
        else:
            label = f"Import {size}"
        icon = "TRACKING_REFINE_BACKWARDS"

    op = layout_row.operator(
        "poliigon.poliigon_model",
        text=label,
        icon=icon,
    )
    op.vAsset = asset_name
    op.vTooltip = tip
    op.vType = asset_type
    op.vLod = lod if len(lod) > 0 else "NONE"
    safe_size_apply(op, size, asset_name)  # has to be set after vType!


def draw_button_model_imported(layout_row: bpy.types.UILayout,
                               asset_data: Dict
                               ) -> None:
    asset_name = asset_data["name"]

    op = layout_row.operator(
        "poliigon.poliigon_select",
        text="Select",
        icon="RESTRICT_SELECT_OFF",
    )
    op.vMode = "model"
    op.vData = asset_name
    op.vTooltip = f"{asset_name}\n(Select all instances)"


def draw_button_texture_local(layout_row: bpy.types.UILayout,
                              asset_data: Dict,
                              error: DisplayError,
                              sizes_in_scene,
                              size_default: str,
                              is_selection: bool
                              ) -> None:
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]

    row_button = layout_row.row(align=True)

    label = "Import " + size_default
    icon = "TRACKING_REFINE_BACKWARDS"
    tooltip = f"{asset_name}\n(Import Material)"
    if len(sizes_in_scene):
        row_button.enabled = is_selection
        label = "Apply " + size_default
        icon = "TRACKING_REFINE_BACKWARDS"
        tooltip = f"{asset_name}\n(Apply Material)"
    elif is_selection:
        label = "Apply " + size_default
        icon = "TRACKING_REFINE_BACKWARDS"
        tooltip = f"{asset_name}\n(Import + Apply Material)"

    if error:
        op = row_button.operator(
            "poliigon.poliigon_material",
            text=error.button_label,
            icon="ERROR",
        )
        op.vTooltip = error.description
    else:
        op = row_button.operator(
            "poliigon.poliigon_material",
            text=label,
            icon=icon,
        )
        op.vTooltip = tooltip

    op.vType = asset_type
    op.vAsset = asset_name
    safe_size_apply(op, size_default, asset_name)
    op.mapping = "UV"
    op.scale = 1.0
    op.use_16bit = cTB.vSettings["use_16"]
    op.reuse_material = True
    op.vData = asset_name + "@" + size_default


def draw_button_texture_imported(layout_row: bpy.types.UILayout,
                                 asset_data: Dict
                                 ) -> None:
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]

    op = layout_row.operator(
        "poliigon.poliigon_apply",
        text="Apply",
        icon="TRACKING_REFINE_BACKWARDS",
    )
    op.vType = asset_type
    op.vAsset = asset_name
    op.vMat = cTB.imported_assets["Textures"][asset_name][0].name
    op.vTooltip = f"{asset_name}\n(Apply to selected models)"


def draw_button_hdri_local(layout_row: bpy.types.UILayout,
                           asset_data: Dict,
                           error: DisplayError,
                           size_default: str
                           ) -> None:
    asset_name = asset_data["name"]

    if error:
        op = layout_row.operator(
            "poliigon.poliigon_hdri",
            text=error.button_label,
            icon="ERROR",
        )
        op.vTooltip = error.description
    else:
        op = layout_row.operator(
            "poliigon.poliigon_hdri",
            text=f"Import {size_default}",
            icon="TRACKING_REFINE_BACKWARDS",
        )
        op.vTooltip = f"{asset_name}\n(Import HDRI)"
    op.vAsset = asset_name
    safe_size_apply(op, size_default, asset_name)
    if cTB.vSettings["hdri_use_jpg_bg"]:
        op.size_bg = f"{cTB.vSettings['hdrib']}_JPG"
    else:
        op.size_bg = f"{size_default}_EXR"


def draw_button_hdri_imported(layout_row: bpy.types.UILayout,
                              asset_data: Dict
                              ) -> None:
    asset_name = asset_data["name"]

    op = layout_row.operator(
        "poliigon.poliigon_hdri",
        text="Apply",
        icon="TRACKING_REFINE_BACKWARDS",
    )
    op.vAsset = asset_name
    # NOTE: Size values will not be used, due to do_apply being set.
    #       Nevertheless the values need to exist in the size enums.
    hdri_size = cTB.vSettings['hdri']
    safe_size_apply(op, hdri_size, asset_name)
    try:
        op.size_bg = f"{hdri_size}_EXR"
    except TypeError as e:
        print(f"Failed to assign bg {hdri_size} for asset {asset_name}: {e})")
    op.do_apply = True
    op.vTooltip = f"{asset_name}\n(Apply to Scene)"


def draw_button_brush_local(layout_row: bpy.types.UILayout,
                            asset_data: Dict,
                            error: DisplayError,
                            size_default: str
                            ) -> None:
    asset_name = asset_data["name"]

    if error:
        op = layout_row.operator(
            "poliigon.poliigon_brush",
            text=error.button_label,
            icon="ERROR",
        )
        op.vTooltip = error.description
    else:
        op = layout_row.operator(
            "poliigon.poliigon_brush",
            text=f"Import {size_default}",
            icon="TRACKING_REFINE_BACKWARDS",
        )
        op.vTooltip = f"{asset_name}\n(Import Brush)"
    op.vAsset = asset_name
    safe_size_apply(op, size_default, asset_name)


def draw_button_brush_imported(layout_row: bpy.types.UILayout,
                               asset_data: Dict,
                               brush_name: str
                               ) -> None:
    asset_name = asset_data["name"]

    if asset_name == brush_name:
        label = "Active"
        tooltip = f"{asset_name}\n(Currently Active Brush)"
    else:
        label = "Activate"
        tooltip = f"{asset_name}\n(Set as Active Brush)"

    op = layout_row.operator(
        "poliigon.poliigon_brush",
        text=label,
        icon="BRUSH_DATA",
    )
    op.vAsset = asset_name
    op.vSize = "apply"
    op.vTooltip = tooltip


def draw_button_download(layout_row: bpy.types.UILayout,
                         asset_data: Dict,
                         error: DisplayError,
                         size_default: str
                         ) -> None:
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]

    if error:
        op = layout_row.operator(
            "poliigon.poliigon_download",
            text=error.button_label,
            icon="ERROR",
        )
        op.vTooltip = error.description
    else:
        op = layout_row.operator(
            "poliigon.poliigon_download",
            text=f"Download {size_default}",
        )
        op.vTooltip = f"{asset_name}\nDownload Default"
        with cTB.lock_download:
            layout_row.enabled = asset_data["id"] not in cTB.vDownloadCancelled

    op.vMode = "download"
    op.vAsset = asset_name
    op.vType = asset_type
    safe_size_apply(op, size_default, asset_name)


def draw_button_purchase(layout_row: bpy.types.UILayout,
                         asset_data: Dict,
                         error: DisplayError,
                         size_default: str,
                         num_credits: int
                         ) -> None:
    asset_id = asset_data["id"]
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]

    thumb_size = THUMB_SIZE_FACTOR[cTB.vSettings["thumbsize"]]

    if error:
        label = error.button_label
    elif num_credits == 0:
        label = "Free"
    elif thumb_size >= 0.75:
        label = "Purchase"
    else:
        label = "Buy"

    icon = 'ERROR' if error else 'NONE'

    if error and error.goto_account is True:
        op = layout_row.operator(
            "poliigon.poliigon_setting",
            text=label,
            icon=icon
        )
        op.vMode = "my_account"
        op.vTooltip = error.description
    else:
        op = layout_row.operator(
            "poliigon.poliigon_download", text=label,
            icon=icon
        )
        op.vAsset = f"{asset_name}@{asset_id}"
        op.vType = asset_type
        safe_size_apply(op, size_default, asset_name)
        op.vMode = "purchase"
        if error:
            op.vTooltip = error.description
        else:
            op.vTooltip = f"Purchase {asset_name}"


def draw_button_quick_menu(layout_row: bpy.types.UILayout,
                           asset_data: Dict,
                           is_downloaded: bool
                           ) -> None:
    asset_id = asset_data["id"]
    asset_name = asset_data["name"]
    asset_type = asset_data["type"]
    asset_sizes = asset_data["sizes"]

    quick_subtitle = "\n(options)" if is_downloaded else "\nSee More"

    op = layout_row.operator(
        "poliigon.show_quick_menu",
        text="",
        icon="TRIA_DOWN",
    )
    op.vAsset = asset_name
    op.vTooltip = f"{asset_name}{quick_subtitle}"
    op.vAssetId = int(asset_id)
    op.vAssetType = asset_type
    op.vSizes = ";".join(asset_sizes)


def draw_missing_grid_dummies(layout_grid: bpy.types.UILayout,
                              sorted_assets: List[Dict],
                              num_columns: int,
                              thumb_width: float
                              ) -> None:
    # Fill rest of grid with empty cells, if needed
    if len(sorted_assets) >= cTB.vSettings["page"]:
        return
    if num_columns == len(sorted_assets):
        num_cols_normal = ceil(cTB.vWidth / thumb_width)
        num_cols_normal = max(1, num_cols_normal)
        num_empty_rows = (cTB.vSettings["page"] // num_cols_normal) - 1
        for _ in range(num_empty_rows):
            layout_grid.column(align=1)
    else:
        for _ in range(len(sorted_assets), cTB.vSettings["page"]):
            layout_grid.column(align=1)


def draw_page_buttons(area: str, idx_page_current: int, at_top: bool = False) -> None:
    num_pages = cTB.vPages[area]

    if num_pages <= 1:
        return

    if not at_top:
        cTB.vBase.separator()

    row = cTB.vBase.row(align=False)

    idx_page_start = 0
    idx_page_end = num_pages

    num_pages_max = int((cTB.vWidth / (30 * cTB.get_ui_scale())) - 5)
    if num_pages > num_pages_max:
        idx_page_start = idx_page_current - int(num_pages_max / 2)
        idx_page_end = idx_page_current + int(num_pages_max / 2)
        if idx_page_start < 0:
            idx_page_start = 0
            idx_page_end = num_pages_max
        elif idx_page_end >= num_pages:
            idx_page_start = num_pages - num_pages_max
            idx_page_end = num_pages

    row_left = row.row(align=True)
    row_left.enabled = idx_page_current != 0

    op = row_left.operator(
        "poliigon.poliigon_setting", text="", icon="TRIA_LEFT"
    )
    op.vMode = "page_-"
    op.vTooltip = "Go to Previous Page"

    row_middle = row.row(align=True)

    op = row_middle.operator(
        "poliigon.poliigon_setting", text="1", depress=(idx_page_current == 0)
    )
    op.vMode = "page_0"
    op.vTooltip = "Go to Page 1"

    if idx_page_start > 1:
        row_middle.label(
            text="",
            icon_value=cTB.vIcons["ICON_dots"].icon_id,
        )

    for idx_page in range(idx_page_start, idx_page_end):
        if idx_page in [0, num_pages - 1]:
            continue

        if at_top:  # buttons get drawn twice, we want to get assets only once
            # Make sure we have data for this page
            cTB.f_GetAssets(area, vPage=idx_page, vBackground=1)

        op = row_middle.operator(
            "poliigon.poliigon_setting",
            text=str(idx_page + 1),
            depress=(idx_page == idx_page_current),
        )
        op.vMode = "page_" + str(idx_page)
        op.vTooltip = "Go to Page " + str(idx_page + 1)

    if idx_page_end < num_pages - 1:
        row_middle.label(text="", icon_value=cTB.vIcons["ICON_dots"].icon_id)

    op = row_middle.operator(
        "poliigon.poliigon_setting",
        text=str(num_pages),
        depress=(idx_page_current == (num_pages - 1)),
    )
    op.vMode = "page_" + str(num_pages - 1)
    op.vTooltip = "Go to Page " + str(num_pages)

    if at_top:  # buttons get drawn twice, we want to get assets only once
        cTB.f_GetAssets(area, vPage=num_pages - 1, vBackground=1)

    row_right = row.row(align=True)
    row_right.enabled = idx_page_current != (num_pages - 1)

    op = row_right.operator(
        "poliigon.poliigon_setting", text="", icon="TRIA_RIGHT"
    )
    op.vMode = "page_+"
    op.vTooltip = "Go to Next Page"

    if at_top:
        cTB.vBase.separator()


def draw_view_more_my_assets(layout_box_not_found: bpy.types.UILayout) -> None:
    if layout_box_not_found is None:
        return

    row = layout_box_not_found.row(align=True)
    row.scale_y = 1.5

    label = "View more online"
    use_padding = 500

    if cTB.vWidth >= use_padding * cTB.get_ui_scale():
        row.label(text="")

    op = row.operator(
        "poliigon.poliigon_setting",
        text=label,
        icon_value=cTB.vIcons["ICON_poliigon"].icon_id
    )
    op.vMode = "view_more"

    if cTB.vWidth >= use_padding * cTB.get_ui_scale():
        row.label(text="")


def draw_view_more_imported(sorted_assets: List[Dict]) -> None:
    if len(sorted_assets) != 0:
        return

    cTB.vBase.separator()
    cTB.vBase.separator()

    if len(cTB.vPurchased):
        row = cTB.vBase.row(align=True)
        op = row.operator(
            "poliigon.poliigon_setting",
            text="Explore Your Assets",
            icon_value=cTB.vIcons["ICON_myassets"].icon_id,
        )
        op.vMode = "area_my_assets"
        op.vTooltip = "Show My Assets"
    else:
        row = cTB.vBase.row(align=True)
        op = row.operator(
            "poliigon.poliigon_setting",
            text="Explore Poliigon Assets",
            icon_value=cTB.vIcons["ICON_poliigon"].icon_id,
        )
        op.vMode = "area_poliigon"
        op.vTooltip = "Show Poliigon Assets"


# @timer
def f_BuildAssets(cTB):
    dbg = 0
    cTB.print_separator(dbg, "f_BuildAssets")

    if not cTB.vCheckScale:
        cTB.check_dpi()

        cTB.vCheckScale = 1

    area = cTB.vSettings["area"]
    idx_page_current = cTB.vPage[area]

    draw_page_buttons(area, idx_page_current, at_top=True)

    sorted_assets = cTB.f_GetAssetsSorted(idx_page_current)

    cTB.print_debug(dbg, "sorted_assets", len(sorted_assets))

    thumb_size_factor = THUMB_SIZE_FACTOR[cTB.vSettings["thumbsize"]]

    category = cTB.vActiveCat[0].replace("All ", "")
    if len(cTB.vActiveCat) > 1:
        category = f"{cTB.vActiveCat[-1]} {category}"

    box_not_found = None
    if not len(sorted_assets):
        box_not_found = build_assets_no_assets(area, category)
    else:
        grid, thumb_width, num_columns = build_assets_prepare_grid(
            thumb_size_factor, sorted_assets)

        is_selection = len(bpy.context.selected_objects) > 0

        idx_asset_start = idx_page_current * cTB.vSettings["page"]
        idx_asset_end = min(idx_asset_start + cTB.vSettings["page"],
                            len(sorted_assets))

        if area == "imported":
            sorted_assets = sorted_assets[idx_asset_start:idx_asset_end]

        name_brush_active = get_active_brush()

        # Build Asset Grid ...
        for idx_asset in range(len(sorted_assets)):
            if idx_asset >= cTB.vSettings["page"]:
                break

            # vAData deliberately not changed to coding style
            # as it is used this way throughout the code base
            vAData = sorted_assets[idx_asset]
            asset_name = vAData["name"]
            asset_type = vAData["type"]

            # See if there's any errors associated with this asset,
            # such as after or during download failure.
            errs = [
                err
                for err in cTB.ui_errors
                if vAData.get("id") and err.asset_id == vAData["id"]
            ]
            error = errs[0] if errs else None
            del errs

            asset_sizes_local = get_local_sizes(vAData)

            is_backplate = cTB.check_backplate(asset_name)

            cTB.f_GetPreview(asset_name)

            is_downloaded = determine_downloaded(vAData)

            size_default = determine_default_size(
                vAData, asset_sizes_local, is_downloaded)
            sizes_in_scene, size_default = determine_in_scene_sizes(
                vAData, size_default)
            size_default = cTB.get_last_downloaded_size(
                asset_name, size_default)

            num_credits = vAData["credits"]

            cell = grid.column(align=True)
            box_thumb = cell.box().column()

            name_row = box_thumb.row(align=True)
            ui_label = "" if "dummy" in asset_name.lower() else asset_name
            name_row.label(text=ui_label)
            name_row.scale_y = 0.8
            name_row.alignment = "CENTER"
            name_row.enabled = False  # To fade label for less contrast.

            draw_thumbnail(vAData, thumb_size_factor, box_thumb)

            row = cell.row(align=True)

            if asset_name == "dummy":
                draw_thumb_state_asset_dummy(row)
            elif cTB.check_if_purchase_queued(vAData.get("id")):
                draw_thumb_state_asset_purchasing(row)
            elif cTB.check_if_download_queued(vAData.get("id")):
                draw_thumb_state_asset_downloading(row, vAData, thumb_width)
            elif asset_name in cTB.vQuickPreviewQueue.keys():
                # TODO(Andreas): When is this branch used???
                #                Looks as if vQuickPreviewQueue is not written to
                draw_thumb_state_asset_downloading_quick_preview(row, vAData)
            elif area in ["poliigon", "my_assets"]:
                if asset_type == "Textures" and asset_name not in cTB.vPurchased:
                    draw_button_quick_preview(
                        row, vAData, is_backplate, is_selection)
                elif asset_name in cTB.vPurchased and area == "poliigon":
                    draw_checkmark_imported(row)

                if asset_name in cTB.vPurchased:
                    if is_downloaded:
                        if asset_type == "Models":
                            draw_button_model_local(row, vAData, error)
                        elif asset_type == "Textures":
                            draw_button_texture_local(row,
                                                      vAData,
                                                      error,
                                                      sizes_in_scene,
                                                      size_default,
                                                      is_selection)
                        elif asset_type == "HDRIs":
                            draw_button_hdri_local(
                                row, vAData, error, size_default)
                        elif asset_type == "Brushes":
                            draw_button_brush_local(
                                row, vAData, error, size_default)
                    else:
                        draw_button_download(row, vAData, error, size_default)
                else:
                    draw_button_purchase(
                        row, vAData, error, size_default, num_credits)

                draw_button_quick_menu(row, vAData, is_downloaded)

            elif area == "imported":
                if asset_type == "Models":
                    draw_button_model_imported(row, vAData)
                elif asset_type == "Textures":
                    draw_button_texture_imported(row, vAData)
                elif asset_type == "HDRIs":
                    draw_button_hdri_imported(row, vAData)
                elif asset_type == "Brushes":
                    draw_button_brush_imported(row, vAData, name_brush_active)

            cell.separator()

        draw_missing_grid_dummies(
            grid, sorted_assets, num_columns, thumb_width)

        draw_page_buttons(area, idx_page_current)

    if area == "my_assets":
        draw_view_more_my_assets(box_not_found)
    elif area == "imported":
        draw_view_more_imported(sorted_assets)


# .............................................................................
# Draw popups
# .............................................................................

def _asset_has_local_blend_file(asset_data: Dict) -> bool:
    if asset_data is None:
        return False
    for path in asset_data["files"]:
        if f_FExt(path) == ".blend":
            return True
    return False


def _asset_has_local_fbx_file(asset_data: Dict) -> bool:
    if asset_data is None:
        return False
    for path in asset_data["files"]:
        if f_FExt(path) == ".fbx" and "_SOURCE" not in os.path.basename(path):
            return True
    return False


def show_quick_menu(cTB, asset_name, asset_id, asset_type, sizes=[]):
    """Generates the quick options menu next to an asset in the UI grid."""

    # Configuration
    if asset_name in cTB.vPurchased:
        title = "Choose Texture Size"  # If downloading and already purchased.
    else:
        title = asset_name

    downloaded = []  # Sizes already downloaded.
    in_scene = False

    asset_data = None
    with cTB.lock_assets:
        if asset_type in cTB.vAssets["local"].keys():
            if asset_name in cTB.vAssets["local"][asset_type].keys():
                asset_data = cTB.vAssets["local"][asset_type][asset_name]
                downloaded = asset_data["sizes"]

    if asset_type in cTB.imported_assets.keys():
        if asset_name in cTB.imported_assets[asset_type].keys():
            in_scene = True

    prefer_blend = cTB.vSettings["download_prefer_blend"]
    link_blend = cTB.link_blend_session
    blend_exists = _asset_has_local_blend_file(asset_data)
    fbx_exists = _asset_has_local_fbx_file(asset_data)
    any_model = blend_exists or fbx_exists
    is_linked_blend_import = prefer_blend and link_blend and blend_exists

    @reporting.handle_draw()
    def draw(self, context):
        layout = self.layout

        # List the different resolution sizes to provide.
        if asset_name in cTB.vPurchased:
            for size in sizes:
                if asset_type == "Textures":
                    draw_material_sizes(context, size, layout)
                elif asset_type == "Models":
                    draw_model_sizes(context, size, layout)
                elif asset_type == "Brushes":
                    draw_brush_sizes(context, size, layout)
                elif asset_type == "HDRIs":
                    draw_hdri_sizes(context, size, layout)
                else:
                    layout.label(text=f"{asset_type} not implemented yet")

            layout.separator()

        ops = layout.operator(
            "poliigon.open_preferences",
            text="Open Import options in Preferences",
            icon="PREFERENCES",
        )
        ops.set_focus = "show_default_prefs"
        layout.separator()

        # Always show view online and high res previews.
        ops = layout.operator(
            "POLIIGON_OT_view_thumbnail",
            text="View larger thumbnail",
            icon="OUTLINER_OB_IMAGE")
        ops.tooltip = f"View larger thumbnail for {asset_name}"
        ops.asset = asset_name
        ops.thumbnail_index = 1

        ops = layout.operator(
            "poliigon.poliigon_link",
            text="View online",
            icon_value=cTB.vIcons["ICON_poliigon"].icon_id,
        )
        ops.vMode = str(asset_id)
        ops.vTooltip = "View on Poliigon.com"

        # If already local, support opening the folder location.
        if downloaded:
            ops = layout.operator(
                "poliigon.poliigon_folder",
                text="Open folder location",
                icon="FILE_FOLDER")
            ops.vAsset = asset_name

            # ... and provide option to sync with asset browser
            in_asset_browser = asset_data.get("in_asset_browser", False)
            is_brush = asset_type == "Brushes"
            is_feature_available = bpy.app.version >= (3, 0)
            missing_local_model = asset_type == "Models" and not any_model
            if not is_brush and is_feature_available and not missing_local_model:
                layout.separator()
                row = layout.row()
                ops = row.operator(
                    "poliigon.update_asset_browser",
                    text="Synchronize with Asset Browser",
                    icon="FILE_REFRESH")
                ops.asset_name = asset_name
                row.enabled = not in_asset_browser and not cTB.lock_client_start.locked()

    def draw_material_sizes(context, size, element):
        """Draw the menu row for a materials' single resolution size."""
        row = element.row()
        imported = f"{asset_name}_{size}" in bpy.data.materials

        if imported or size in downloaded:
            # Action: Load and apply it
            if imported:
                label = f"{size} (apply material)"
                tip = f"Apply {size} Material\n{asset_name}"
            elif context.selected_objects:
                label = f"{size} (import + apply)"
                tip = f"Apply {size} Material\n{asset_name}"
            else:
                label = f"{size} (import)"
                tip = f"Import {size} Material\n{asset_name}"

            # If nothing is selected and this size is already importing,
            # then there's nothing to do.
            if imported and not context.selected_objects:
                row.enabled = False

            ops = row.operator(
                "poliigon.poliigon_material",
                text=label,
                icon="TRACKING_REFINE_BACKWARDS")
            # Order is relevant here. vType needs to be set before vSize!
            ops.vAsset = asset_name
            ops.vType = asset_type
            safe_size_apply(ops, size, asset_name)
            ops.mapping = "UV"
            ops.scale = 1.0
            ops.use_16bit = cTB.vSettings["use_16"]
            ops.reuse_material = True
            ops.vTooltip = tip

        else:
            # Action: Download
            label = f"{size} (download)"
            ops = row.operator(
                "poliigon.poliigon_download",
                text=label,
                icon="IMPORT")
            ops.vAsset = asset_name
            safe_size_apply(ops, size, asset_name)
            ops.vType = asset_type
            ops.vMode = "download"
            ops.vTooltip = f"Download {size} Material\n{asset_name}"

    def draw_model_sizes(context, size, element):
        """Draw the menu row for a model's single resolution size."""
        row = element.row()

        if size in downloaded and any_model:
            # Action: Load and apply it
            lod, label, tip = get_model_op_details(asset_name,
                                                   asset_type,
                                                   size)
            if is_linked_blend_import:
                label += " (disable link .blend to import size)"

            ops = row.operator(
                "poliigon.poliigon_model",
                text=label,
                icon="TRACKING_REFINE_BACKWARDS")
            ops.vAsset = asset_name
            ops.vType = asset_type
            safe_size_apply(ops, size, asset_name)
            ops.vTooltip = tip
            ops.vLod = lod if len(lod) > 0 else "NONE"
            row.enabled = not is_linked_blend_import
        else:
            # Action: Download
            ops = row.operator(
                "poliigon.poliigon_download",
                text=f"{size} (download)",
                icon="IMPORT")
            ops.vAsset = asset_name
            safe_size_apply(ops, size, asset_name)
            ops.vType = asset_type
            ops.vMode = "download"
            ops.vTooltip = f"Download {size} textures\n{asset_name}"

    def draw_hdri_sizes(context, size, element):
        """Draw the menu row for an HDRI's single resolution size."""
        row = element.row()

        size_light = ""
        if in_scene:
            image_name_light = asset_name + "_Light"
            if image_name_light in bpy.data.images.keys():
                path_light = bpy.data.images[image_name_light].filepath
                filename = os.path.basename(path_light)
                match_object = re.search(r"_(\d+K)[_\.]", filename)
                size_light = match_object.group(1) if match_object else cTB.vSettings['hdri']

        if size in downloaded:
            # Action: Load and apply it
            if size == size_light:
                label = f"{size} (apply HDRI)"
                tip = f"Apply {size} HDRI\n{asset_name}"
            else:
                label = f"{size} (import HDRI)"
                tip = f"Import {size} HDRI\n{asset_name}"

            ops = row.operator(
                "poliigon.poliigon_hdri",
                text=label,
                icon="TRACKING_REFINE_BACKWARDS")
            ops.vAsset = asset_name
            safe_size_apply(ops, size, asset_name)
            if cTB.vSettings["hdri_use_jpg_bg"]:
                ops.size_bg = f"{cTB.vSettings['hdrib']}_JPG"
            else:
                ops.size_bg = f"{size}_EXR"
            ops.vTooltip = tip

        else:
            # Action: Download
            label = f"{size} (download)"
            ops = row.operator(
                "poliigon.poliigon_download",
                text=label,
                icon="IMPORT")
            ops.vAsset = asset_name
            ops.vType = asset_type
            safe_size_apply(ops, size, asset_name)
            ops.vMode = "download"
            ops.vTooltip = f"Download {size}\n{asset_name}"

    def draw_brush_sizes(context, size, element):
        """Draw the menu row for a brush's single resolution size."""
        row = element.row()
        if in_scene or size in downloaded:
            # Action: Load and apply it
            if in_scene:
                label = f"{size} (equip brush)"
                tip = f"Equip {size} brush\n{asset_name}"
            else:
                label = f"{size} (import brush)"
                tip = f"Equip {size} brush\n{asset_name}"

            ops = row.operator(
                "poliigon.poliigon_brush",
                text=label,
                icon="TRACKING_REFINE_BACKWARDS")
            ops.vAsset = asset_name
            safe_size_apply(ops, size, asset_name)
            ops.vTooltip = tip

        else:
            # Action: Download
            label = f"{size} (download)"
            ops = row.operator(
                "poliigon.poliigon_download",
                text=label,
                icon="IMPORT")
            ops.vAsset = asset_name
            ops.vType = asset_type
            safe_size_apply(ops, size, asset_name)
            ops.vMode = "download"
            ops.vTooltip = f"Download {size}\n{asset_name}"

    # Generate the popup menu.
    bpy.context.window_manager.popup_menu(draw, title=title, icon="QUESTION")


def show_categories_menu(cTB, categories, index):
    """Generates the popup menu to display category selection options."""

    @reporting.handle_draw()
    def draw(self, context):
        layout = self.layout
        row = layout.row()
        col = row.column(align=True)

        for i in range(len(categories)):
            if i > 0 and i % 15 == 0:
                col = row.column(align=True)

            button = categories[i]
            label = f" {button}"
            op = col.operator("poliigon.poliigon_setting", text=label)
            op.vMode = f"category_{index}_{button}"
            op.vTooltip = f"Select {button}"

            if i == 0:
                col.separator()

    bpy.context.window_manager.popup_menu(draw)


# TODO(Andreas): Looks like this dropdown is never used.
def f_DropdownDownloadTexture(cTB,
                              buttons: List[str],
                              tooltips: List[str],
                              asset_type: str,
                              asset_name: str
                              ) -> None:
    dbg = 0

    cTB.print_separator(dbg, "f_DropdownDownloadTexture")
    if bpy.app.background:
        return  # Don't popup menus when running headless.

    cTB.check_dpi()

    title = "Choose Extra Texture Size to Download :"
    command = "poliigon.poliigon_download"

    @reporting.handle_draw()
    def draw(self, context):
        cTB.print_debug(dbg, "f_DropdownDownloadTexture", kwargs)

        layout = self.layout
        row = layout.row()
        col = row.column(align=True)

        for idx_button, label_button in enumerate(buttons):
            if idx_button > 0 and idx_button % 30 == 0:
                col = row.column(align=True)

            cTB.print_debug(dbg, "f_DropdownDownloadTexture", label_button)

            if label_button == "-":
                col.separator()
                continue

            row_button = col.row(align=True)

            label = f" {label_button}  (download)"
            icon = None
            with cTB.lock_assets:
                if asset_type in cTB.vAssets["local"].keys():
                    # TODO(Andreas): While refactoring: This looks fishy, doesn't match first if
                    if asset_name in cTB.vAssets[cTB.vSettings["area"]][asset_type].keys():
                        if asset_type == "Textures" and label_button == cTB.vSettings["res"]:
                            label = f" {label_button}  (download default)"
                        elif asset_type == "Models" and label_button == cTB.vSettings["mres"]:
                            label = f" {label_button}  (download default)"
                        elif asset_type == "HDRIs" and label_button == cTB.vSettings["hdri"]:
                            label = f" {label_button}  (download default)"

                icon = "IMPORT"

            if icon is None:
                op = row_button.operator(command, text=label)
            else:
                op = row_button.operator(command, text=label, icon=icon)
            op.vMode = "download"
            op.vAsset = asset_name
            op.vType = asset_type
            try:
                op.vSize = label_button.split(" ")[0]
            except TypeError as e:
                print(f"Failed to assign size for asset {asset_name}: {e})")

            tooltip = None
            if len(tooltips) > idx_button:
                tooltip = tooltips[idx_button]
            if tooltip is not None:
                op.vTooltip = tooltip

    bpy.context.window_manager.popup_menu(draw, title=title, icon="QUESTION")


# TODO(Andreas): Looks like this dropdown is never used.
def f_DropdownAssetOptions(cTB,
                           buttons: List[str],
                           data: str
                           ) -> None:
    dbg = 0

    cTB.print_separator(dbg, "f_DropdownAssetOptions")
    if bpy.app.background:
        return  # Don't popup menus when running headless.

    cTB.check_dpi()

    title = ""
    command = "poliigon.poliigon_asset_options"

    @reporting.handle_draw()
    def draw(self, context):
        cTB.print_debug(dbg, "f_DropdownAssetOptions", kwargs)

        layout = self.layout
        row = layout.row()
        col = row.column(align=True)

        # TODO(Andreas): I doubt this ever worked as vType is not set,
        #                when this drop down is called
        #if command == "poliigon.poliigon_asset_options":
        #    with cTB.lock_assets:
        #        vSizes = cTB.vAssets[cTB.vSettings["area"]][vType][data]["sizes"]
        #    if len(vSizes):
        #        vSizes.sort()
        #        col.label(text=data + "  (" + ",".join(vSizes) + ")")
        #    else:
        #        col.label(text=data)
        #    col.separator()

        for idx_button, label_button in enumerate(buttons):
            if idx_button > 0 and idx_button % 30 == 0:
                col = row.column(align=True)

            cTB.print_debug(dbg, "f_DropdownAssetOptions", label_button)

            if label_button == "-":
                col.separator()
                continue

            row_button = col.row(align=True)

            icon = None
            if label_button == "Open Asset Folder(s)":
                icon = "FILE_FOLDER"
            elif label_button == "Find Asset on Poliigon.com":
                icon = "URL"

            label = f" {label_button}"

            if icon is None:
                op = row_button.operator(command, text=label)
            elif icon == "URL":
                op = row_button.operator(
                    command,
                    text=label,
                    icon_value=cTB.vIcons["ICON_poliigon"].icon_id,
                )
            else:
                op = row_button.operator(command, text=label, icon=icon)

            if label_button == "Open Asset Folder(s)":
                op.vData = f"{data}@dir"
            elif label_button == "Find Asset on Poliigon.com":
                op.vData = f"{data}@link"

    bpy.context.window_manager.popup_menu(draw, title=title, icon="QUESTION")


# TODO(Andreas): Looks like this dropdown is never used.
def f_DropdownPreset(cTB,
                     buttons: List[str],
                     data: str
                     ) -> None:
    dbg = 0

    cTB.print_separator(dbg, "f_DropdownPreset")
    if bpy.app.background:
        return  # Don't popup menus when running headless.

    cTB.check_dpi()

    title = ""
    command = "poliigon.poliigon_preset"

    @reporting.handle_draw()
    def draw(self, context):
        cTB.print_debug(dbg, "f_DropdownPreset", kwargs)

        layout = self.layout
        row = layout.row()
        col = row.column(align=True)

        is_selection = len(bpy.context.selected_objects) > 0

        for idx_button, label_button in enumerate(buttons):
            if idx_button > 0 and idx_button % 30 == 0:
                col = row.column(align=True)

            cTB.print_debug(dbg, "f_DropdownPreset", label_button)

            if label_button == "-":
                col.separator()
                continue

            row_button = col.row(align=True)

            if label_button == "Real World" and not is_selection:
                continue

            icon = None
            if "Import" in label_button:
                icon = "TRACKING_REFINE_BACKWARDS"

            label = f" {label_button}"

            if icon is None:
                op = row_button.operator(command, text=label)
            else:
                op = row_button.operator(command, text=label, icon=icon)
            op.vData = f"{data}@{label_button}"

    bpy.context.window_manager.popup_menu(draw, title=title, icon="QUESTION")


# TODO(Andreas): Looks like this dropdown is never used.
def f_DropdownTexture(cTB,
                      buttons: List[str],
                      asset_type: str,
                      data: str,
                      tooltips: List[str] = []
                      ) -> None:
    dbg = 0

    cTB.print_separator(dbg, "f_DropdownTexture")
    if bpy.app.background:
        return  # Don't popup menus when running headless.

    cTB.check_dpi()

    title = ""
    command = "poliigon.poliigon_texture"

    @reporting.handle_draw()
    def draw(self, context):
        cTB.print_debug(dbg, "f_DropdownTexture", kwargs)

        layout = self.layout
        row = layout.row()
        col = row.column(align=True)

        for idx_button, label_button in enumerate(buttons):
            if idx_button > 0 and idx_button % 30 == 0:
                col = row.column(align=True)

            cTB.print_debug(dbg, "f_DropdownTexture", label_button)

            if label_button == "-":
                col.separator()
                continue

            row_button = col.row(align=True)

            tooltip = None
            if len(tooltips) > idx_button:
                tooltip = tooltips[idx_button]

            label = f" {label_button}"

            op = row_button.operator(command, text=label)
            op.vType = asset_type
            op.vData = f"{data}@{label_button}"
            if tooltip is not None:
                op.vTooltip = tooltip

    bpy.context.window_manager.popup_menu(draw, title=title, icon="QUESTION")


# TODO(Andreas): Looks like this dropdown is never used.
def f_DropdownApply(cTB,
                    buttons: List[str],
                    asset_type: str,
                    asset_name: str
                    ) -> None:
    dbg = 0

    cTB.print_separator(dbg, "f_DropdownApply")
    if bpy.app.background:
        return  # Don't popup menus when running headless.

    cTB.check_dpi()

    title = ""
    command = "poliigon.poliigon_apply"

    @reporting.handle_draw()
    def draw(self, context):
        cTB.print_debug(dbg, "f_DropdownApply", kwargs)

        layout = self.layout
        row = layout.row()
        col = row.column(align=True)

        for idx_button, label_button in enumerate(buttons):
            if idx_button > 0 and idx_button % 30 == 0:
                col = row.column(align=True)

            cTB.print_debug(dbg, "f_DropdownApply", label_button)

            if label_button == "-":
                col.separator()
                continue

            row_button = col.row(align=True)

            label = f" {label_button}"

            op = row_button.operator(command, text=label)
            op.vType = asset_type
            # TODO(Andreas): I doubt this ever worked as vData is not set,
            #                when this drop down is called
            # op.vAsset = vData
            op.vMat = label_button

    bpy.context.window_manager.popup_menu(draw, title=title, icon="QUESTION")


# TODO(Andreas): Looks like this dropdown is never used.
def f_DropdownSelect(cTB,
                     buttons: List[str],
                     mode: str,
                     data: str
                     ) -> None:
    dbg = 0

    cTB.print_separator(dbg, "f_DropdownSelect")
    if bpy.app.background:
        return  # Don't popup menus when running headless.

    cTB.check_dpi()

    title = ""
    command = "poliigon.poliigon_select"

    @reporting.handle_draw()
    def draw(self, context):
        cTB.print_debug(dbg, "f_DropdownSelect", kwargs)

        layout = self.layout
        row = layout.row()
        col = row.column(align=True)

        for idx_button, label_button in enumerate(buttons):
            if idx_button > 0 and idx_button % 30 == 0:
                col = row.column(align=True)

            cTB.print_debug(dbg, "f_DropdownSelect", label_button)

            if label_button == "-":
                col.separator()
                continue

            row_button = col.row(align=True)

            label = f" {label_button}"

            op = row_button.operator(command, text=label)
            op.vMode = mode
            op.vData = f"{data}@{label_button}"

    bpy.context.window_manager.popup_menu(draw, title=title, icon="QUESTION")


def f_Popup(cTB, vTitle="", vMsg="", vBtns=["OK"], vCmds=[None], vMode=None):
    dbg = 0
    cTB.print_separator(dbg, "f_Popup")

    @reporting.handle_draw()
    def draw(self, context):
        vLayout = self.layout

        vCol = vLayout.column(align=True)

        vIcon = "INFO"
        if vMode == "question":
            vIcon = "QUESTION"
        elif vMode == "error":
            vIcon = "ERROR"

        vCol.label(text=vTitle, icon=vIcon)

        vCol.separator()

        vCol.label(text=vMsg)

        vCol.separator()
        vCol.separator()

        vRow = vCol.row()
        for i in range(len(vBtns)):
            if vCmds[i] in [None, "cancel"]:
                vOp = vRow.operator("poliigon.poliigon_setting", text=vBtns[i])
                vOp.vMode = "none"

            elif vCmds[i] == "credits":
                vOp = vRow.operator(
                    "poliigon.poliigon_link", text="Add Credits", depress=1
                )
                vOp.vMode = "credits"

    bpy.context.window_manager.popover(draw)


# TODO(Andreas): Function not in use (only called from operator
#                poliigon.poliigon_active in mode "info", which is not used)
def f_AssetInfo(cTB, vAsset):
    """Dynamic menu popup call populated based on info on this asset."""
    dbg = 0
    cTB.print_separator(dbg, "f_AssetInfo")

    @reporting.handle_draw()
    def asset_info_draw(self, context):
        """Called as part of the popup in operators for info mode."""
        vAssetType = cTB.vSettings["category"][cTB.vSettings["area"]][0]

        with cTB.lock_assets:
            if cTB.vSettings["area"] == "poliigon":
                vAData = cTB.vAssets["poliigon"][vAssetType][vAsset]
            else:
                vAData = cTB.vAssets[vAssetType][vAsset]

        vLayout = self.layout
        vLayout.alignment = "CENTER"

        # .................................................................

        vCol = vLayout.column(align=True)

        with cTB.lock_previews:
            vCol.template_icon(icon_value=cTB.vPreviews[vAsset].icon_id, scale=10)

        # .................................................................

        vRow = vCol.row(align=False)

        vRow.label(text=vAsset)

        vOp = vRow.operator(
            "poliigon.poliigon_asset_options", text="", icon="FILE_FOLDER"
        )
        vOp.vType = cTB.vActiveType
        vOp.vData = vAsset + "@dir"
        vOp.vTooltip = "Open " + vAsset + " Folder(s)"

        vOp = vRow.operator(
            "poliigon.poliigon_link",
            text="",
            icon_value=cTB.vIcons["ICON_poliigon"].icon_id,
        )
        vOp.vMode = str(vAData["id"])
        vOp.vTooltip = "View on Poliigon.com"

        vCol.separator()

        # .................................................................

        if vAssetType == "Models":
            vCol.label(text="Models :")

            vCol.separator()

        # .................................................................

        vCol.label(text="Maps :")

        vGrid = vCol.box().grid_flow(
            row_major=1, columns=4, even_columns=0, even_rows=0, align=False
        )

        for vM in vAData["maps"]:
            vGrid.label(text=vM)

        vCol.separator()

        # .................................................................

        vCol.label(text="Map Sizes :")

        vCol.box().label(text="   ".join(vAData["sizes"]))

        vCol.separator()

    bpy.context.window_manager.popover(asset_info_draw, ui_units_x=15)


@reporting.handle_draw()
def f_NotificationBanner(notifications, layout):
    """General purpose notification banner UI draw element."""

    def build_mode(url, action, notification_id):
        return "notify@{}@{}@{}".format(url, action, notification_id)

    if not notifications:
        return

    box = layout.box()
    row = box.row(align=True)
    main_col = row.column(align=True)

    panel_width = cTB.vWidth / (cTB.get_ui_scale() or 1)

    for i, notice in enumerate(notifications):
        first_row = main_col.row(align=False)
        x_row = first_row  # x_row is the row to add the x button to, if there.

        cTB.notification_signal_view(notice)

        if notice.action == Notification.ActionType.OPEN_URL:
            # Empirical for width for "Beta addon: [Take survey]" specifically.
            single_row_width = 250
            if panel_width > single_row_width:
                # Single row with text + button.
                # TODO: generalize this for notification message and length,
                # and if dismiss is included.
                # During SOFT-780 this has been changed for POPUP_MESSAGE in a
                # very simplistic way
                # (commit: https://github.com/poliigon/poliigon-addon-blender/pull/278/commits/00296ab70288893a023a6705d52eb4505ce36897).
                # When addressing this properly,
                # make sure to address it for all notification types.
                first_row.alert = True
                first_row.label(text=notice.title)
                first_row.alert = False
                ops = first_row.operator(
                    "poliigon.poliigon_link",
                    icon=notice.icon or "NONE",
                    text=notice.ac_open_url_label,
                )
                if notice.tooltip:
                    ops.vTooltip = notice.tooltip
                ops.vMode = build_mode(
                    notice.ac_open_url_address,
                    notice.ac_open_url_label,
                    notice.notification_id)

            else:
                # Two rows (or more, if text wrapping).
                col = first_row.column(align=True)
                col.alert = True
                # Empirically found squaring below worked best for 1 & 2x displays,
                # which accounts for the box+panel padding and the 'x' button.
                if notice.allow_dismiss:
                    padding_width = 32 * cTB.get_ui_scale()
                else:
                    padding_width = 17 * cTB.get_ui_scale()
                cTB.f_Label(cTB.vWidth - padding_width, notice.title, col)
                col.alert = False

                second_row = main_col.row(align=True)
                second_row.scale_y = 1.0
                ops = second_row.operator(
                    "poliigon.poliigon_link",
                    icon=notice.icon or "NONE",
                    text=notice.ac_open_url_label,
                )
                if notice.tooltip:
                    ops.vTooltip = notice.tooltip
                ops.vMode = build_mode(
                    notice.ac_open_url_address,
                    notice.ac_open_url_label,
                    notice.notification_id)

        elif notice.action == Notification.ActionType.UPDATE_READY:
            # Empirical for width for "Update ready: Download | logs".
            single_row_width = 300
            if panel_width > single_row_width:
                # Single row with text + button.
                first_row.alert = True
                first_row.label(text=notice.title)
                first_row.alert = False
                splitrow = first_row.split(factor=0.7, align=True)
                splitcol = splitrow.split(align=True)

                ops = splitcol.operator(
                    "poliigon.poliigon_link",
                    icon=notice.icon or "NONE",
                    text=str(notice.ac_update_ready_download_label),
                )
                if notice.tooltip:
                    ops.vTooltip = notice.tooltip
                ops.vMode = build_mode(
                    notice.ac_update_ready_download_url,
                    notice.ac_update_ready_download_label,
                    notice.notification_id)

                splitcol = splitrow.split(align=True)
                ops = splitcol.operator(
                    "poliigon.poliigon_link",
                    text=str(notice.ac_update_ready_logs_label),
                )
                if notice.tooltip:
                    ops.vTooltip = "See changes in this version"
                ops.vMode = build_mode(
                    notice.ac_update_ready_logs_url,
                    notice.ac_update_ready_logs_label,
                    notice.notification_id)
            else:
                # Two rows (or more, if text wrapping).
                col = first_row.column(align=True)
                col.alert = True
                if notice.allow_dismiss:
                    padding_width = 32 * cTB.get_ui_scale()
                else:
                    padding_width = 17 * cTB.get_ui_scale()
                cTB.f_Label(cTB.vWidth - padding_width, notice.title, col)
                col.alert = False

                second_row = main_col.row(align=True)
                splitrow = second_row.split(factor=0.7, align=True)
                splitcol = splitrow.split(align=True)
                ops = splitcol.operator(
                    "poliigon.poliigon_link",
                    icon=notice.icon or "NONE",
                    text=str(notice.ac_update_ready_download_label),
                )
                if notice.tooltip:
                    ops.vTooltip = notice.tooltip
                ops.vMode = build_mode(
                    notice.ac_update_ready_download_url,
                    notice.ac_update_ready_download_label,
                    notice.notification_id)
                splitcol = splitrow.split(align=True)
                ops = splitcol.operator(
                    "poliigon.poliigon_link",
                    text=str(notice.ac_update_ready_logs_label),
                )
                if notice.tooltip:
                    ops.vTooltip = notice.tooltip
                ops.vMode = build_mode(
                    notice.ac_update_ready_logs_url,
                    notice.ac_update_ready_logs_label,
                    notice.notification_id)

        elif notice.action == Notification.ActionType.POPUP_MESSAGE:
            single_row_width = 250
            if panel_width > single_row_width and len(notice.title) <= 80:
                # Single row with text + button.
                first_row.alert = notice.ac_popup_message_alert
                first_row.label(text=notice.title)
                first_row.alert = False
                ops = first_row.operator(
                    "poliigon.popup_message",
                    icon=notice.icon or "NONE",
                    text="View"
                )

            else:
                # Two rows (or more, if text wrapping).
                col = first_row.column(align=True)
                col.alert = notice.ac_popup_message_alert
                # Empirically found squaring below worked best for 1 & 2x displays,
                # which accounts for the box+panel padding and the 'x' button.
                if notice.allow_dismiss:
                    padding_width = 32 * cTB.get_ui_scale()
                else:
                    padding_width = 17 * cTB.get_ui_scale()
                cTB.f_Label(cTB.vWidth - padding_width, notice.title, col)
                col.alert = False

                second_row = main_col.row(align=True)
                second_row.scale_y = 1.0
                ops = second_row.operator(
                    "poliigon.popup_message",
                    icon=notice.icon or "NONE",
                    text="View",
                )

            ops.message_body = notice.ac_popup_message_body
            ops.notice_id = notice.notification_id
            if notice.tooltip:
                ops.vTooltip = notice.tooltip
            if notice.ac_popup_message_url:
                ops.message_url = notice.ac_popup_message_url

        elif notice.action == Notification.ActionType.RUN_OPERATOR:
            # Single row with only a button.
            ops = first_row.operator(
                "poliigon.notice_operator",
                text=notice.title,
                icon=notice.icon or "NONE",
            )
            ops.notice_id = notice.notification_id
            ops.ops_name = notice.ac_run_operator_ops_name
            ops.vTooltip = notice.tooltip

        else:
            main_col.label(text=notice.title)
            print("Invalid notifcation type")

        if notice.allow_dismiss:
            right_col = x_row.column(align=True)
            ops = right_col.operator(
                "poliigon.close_notification", icon="X", text="", emboss=False)
            ops.notification_index = i

    layout.separator()


# .............................................................................
# Draw panel
# .............................................................................


class POLIIGON_PT_toolbox(Panel):
    bl_idname = "POLIIGON_PT_toolbox"
    bl_label = "Poliigon"
    bl_category = "Poliigon"
    bl_space_type = "VIEW_3D"
    bl_region_type = "UI"

    @reporting.handle_draw()
    def draw(self, context):
        f_BuildUI(self, context)


# .............................................................................
# Shader editor Shift-A menu
# .............................................................................

def append_poliigon_groups_node_add(self, context):
    """Appending to add node menu, for Poliigon node groups"""
    self.layout.menu('POLIIGON_MT_add_node_groups')


class POLIIGON_MT_add_node_groups(bpy.types.Menu):
    """Menu for the Poliigon Shader node groups"""

    bl_space_type = 'NODE_EDITOR'
    bl_label = "Poliigon Node Groups"

    def draw(self, context):
        layout = self.layout
        col = layout.column(align=True)
        if bpy.app.version >= (2, 90):
            col.operator("poliigon.add_converter_node",
                         text="Mosaic"
                         ).node_type = "Mosaic_UV_Mapping"
        col.operator("poliigon.add_converter_node",
                     text="PBR mixer"
                     ).node_type = "Poliigon_Mixer"

        col.separator()


# .............................................................................
# Utilities
# .............................................................................


def get_model_op_details(asset_name, asset_type, size):
    """Get details to use in the ui for a given model and size."""
    default_lod = cTB.vSettings["lod"]
    with cTB.lock_assets:
        asset_data = cTB.vAssets["local"][asset_type][asset_name]
    downloaded = asset_data["sizes"]

    if len(asset_data["lods"]):
        lod = cTB.f_GetClosestLod(asset_data["lods"], default_lod)
    else:
        lod = ""

    coll_name = construct_model_name(asset_name, size, lod)

    coll = bpy.data.collections.get(coll_name)
    if coll:
        in_scene = True
    else:
        in_scene = False

    label = ""
    tip = ""
    if size in downloaded:
        if in_scene:
            if lod:
                label = f"{size} {lod} (import again)"
                tip = f"Import {size} {lod} again\n{asset_name}"
            else:
                label = f"{size} (import again)"
                tip = f"Import {size} again\n{asset_name}"
        else:
            if lod:
                label = f"{size} {lod} (import)"
                tip = f"Import {size} {lod}\n{asset_name}"
            else:
                label = f"{size} (import)"
                tip = f"Import {size}\n{asset_name}"

    return lod, label, tip


# .............................................................................
# Registration
# .............................................................................

classes = (
    POLIIGON_PT_toolbox,
    POLIIGON_MT_add_node_groups
)


def register():
    for cls in classes:
        bpy.utils.register_class(cls)

    bpy.types.NODE_MT_add.append(append_poliigon_groups_node_add)


def unregister():
    bpy.types.NODE_MT_add.remove(append_poliigon_groups_node_add)

    for cls in reversed(classes):
        bpy.utils.unregister_class(cls)
