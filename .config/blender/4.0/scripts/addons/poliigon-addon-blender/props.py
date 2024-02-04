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


from bpy.props import (
    FloatProperty,
    PointerProperty,
    StringProperty,
)
import bpy.utils.previews


class PoliigonUserProps(bpy.types.PropertyGroup):
    vEmail: StringProperty(
        name="", description="Your Email", options={"SKIP_SAVE"}
    )
    vPassShow: StringProperty(
        name="", description="Your Password", options={"SKIP_SAVE"}
    )
    vPassHide: StringProperty(
        name="",
        description="Your Password",
        options={"HIDDEN","SKIP_SAVE"},
        subtype="PASSWORD",
    )
    search_poliigon: StringProperty(
        name="",
        default="",
        description="Search Poliigon Assets",
        options={"SKIP_SAVE"},
    )
    search_my_assets: StringProperty(
        name="",
        default="",
        description="Search My Assets",
        options={"SKIP_SAVE"},
    )
    search_imported: StringProperty(
        name="",
        default="",
        description="Search Imported Assets",
        options={"SKIP_SAVE"},
    )


class PoliigonMaterialProps(bpy.types.PropertyGroup):
    """Properties saved to an individual material"""

    # Properties common to all asset types
    asset_name: bpy.props.StringProperty()
    asset_id: bpy.props.IntProperty(default=-1)
    asset_type: bpy.props.StringProperty()

    # Material specific properties
    size: bpy.props.StringProperty()  # resolution, but kept size name to stay consistent
    mapping: bpy.props.StringProperty()
    scale: bpy.props.FloatProperty()
    displacement: bpy.props.FloatProperty()
    use_16bit: bpy.props.BoolProperty(default=False)
    use_micro_displacements: bpy.props.BoolProperty(default=False)
    is_backplate: bpy.props.BoolProperty(default=False)


class PoliigonObjectProps(bpy.types.PropertyGroup):
    """Properties saved to an individual object"""

    # Properties common to all asset types
    asset_name: bpy.props.StringProperty()
    asset_id: bpy.props.IntProperty(default=-1)
    asset_type: bpy.props.StringProperty()

    # Model/object specific properties
    lod: bpy.props.StringProperty()
    use_collection: bpy.props.BoolProperty(default=False)
    link_blend: bpy.props.BoolProperty(default=False)


class PoliigonWorldProps(bpy.types.PropertyGroup):
    """Properties saved to an individual world shader"""

    # Properties common to all asset types
    asset_name: bpy.props.StringProperty()
    asset_id: bpy.props.IntProperty(default=-1)
    asset_type: bpy.props.StringProperty()

    # HDRI specific properties
    size: bpy.props.StringProperty()
    size_bg: bpy.props.StringProperty()
    hdr_strength: FloatProperty()
    rotation: FloatProperty()


classes = (
    PoliigonUserProps,
    PoliigonMaterialProps,
    PoliigonObjectProps,
    PoliigonWorldProps
)


def register():
    for cls in classes:
        bpy.utils.register_class(cls)

    bpy.types.WindowManager.poliigon_props = PointerProperty(
        type=PoliigonUserProps
    )
    bpy.types.Material.poliigon_props = PointerProperty(
        type=PoliigonMaterialProps
    )
    bpy.types.Object.poliigon_props = PointerProperty(
        type=PoliigonObjectProps
    )
    bpy.types.World.poliigon_props = PointerProperty(
        type=PoliigonWorldProps
    )

    bpy.types.Scene.vEditText = StringProperty(default="")
    bpy.types.Scene.vEditMatName = StringProperty(default="")
    bpy.types.Scene.vDispDetail = FloatProperty(default=1.0, min=0.1, max=10.0)

    bpy.types.Material.poliigon = StringProperty(default="", options={"HIDDEN"})
    bpy.types.Object.poliigon = StringProperty(default="", options={"HIDDEN"})
    bpy.types.Object.poliigon_lod = StringProperty(default="", options={"HIDDEN"})
    bpy.types.Image.poliigon = StringProperty(default="", options={"HIDDEN"})

    bpy.context.window_manager.poliigon_props.vEmail = ""
    bpy.context.window_manager.poliigon_props.vPassShow = ""
    bpy.context.window_manager.poliigon_props.vPassHide = ""
    bpy.context.window_manager.poliigon_props.search_poliigon = ""
    bpy.context.window_manager.poliigon_props.search_my_assets = ""
    bpy.context.window_manager.poliigon_props.search_imported = ""


def unregister():
    del bpy.types.Scene.vDispDetail
    del bpy.types.Scene.vEditMatName
    del bpy.types.Scene.vEditText

    del bpy.types.World.poliigon_props
    del bpy.types.Object.poliigon_props
    del bpy.types.Material.poliigon_props
    del bpy.types.WindowManager.poliigon_props

    for cls in reversed(classes):
        bpy.utils.unregister_class(cls)
