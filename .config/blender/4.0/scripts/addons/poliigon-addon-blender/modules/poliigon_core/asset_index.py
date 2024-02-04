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

"""Module for managing and caching asset data."""
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Callable
import gzip
import json
import logging
import os
import re
import time

from poliigon_core import api
from poliigon_core import assets
from poliigon_core import logger


# Compiled regex to avoid re-instancing each time.
# Checks for preview being in the last section of a filename split into _'s
# The [^_]* means match any character except another _, and $ asserts it's at
# the end of the match string.
_PREVIEW_PATTERN = re.compile(r"_[^_]*preview[^_]*$", re.IGNORECASE)


class AssetIndex():
    all_assets: Dict[int, assets.AssetData]

    # A generic way to save multiple queries. How to know when to clear them though?
    # Maybe they are always cleared if you need to re load them, but they stay
    # in tact if all you are doing is changes to sorting.
    # Also worth acknwoledging that, in this context, paging will indeed matter
    # and creates a tighter (than desired) coupling to the front end. Something
    # to think about during development.
    cached_queries: Dict[str, List[int]]  # {query_tuple: [asset_ids]}

    path_cache: str

    # TODO(Joao): instantiate reporting callable in the addon_core class
    # function from the reporting addon side to report Sentry messages from
    # asset index functions. Expected to receive as parameter the error tag
    # and the error message
    reporting_callable: Optional[Callable] = None

    def __init__(self,
                 path_cache: str = "",
                 log: Optional[logging.Logger] = None):
        if log is None:
            self.logger = logger.initialize_logger(
                "PoliigonAddon.AssetIndex", log_lvl=logger.ERROR)
        else:
            self.logger = log
        self.path_cache = path_cache
        self.all_assets = {}
        self.cached_queries = {}

    def capture_message(self,
                        message: str,
                        code_msg: str = None,
                        level: str = "error",
                        max_reports: int = 10
                        ) -> None:
        if self.reporting_callable is None:
            return
        self.reporting_callable(message, code_msg, level, max_reports)

    @staticmethod
    def _filter_image_urls(urls: List[str]) -> List[str]:
        return [url for url in urls if ".png" in url.lower() or ".jpg" in url.lower()]

    @staticmethod
    def _get_texture_real_world_dimension(
            asset_dict: Dict) -> Optional[Tuple[float, float]]:
        dimension_height = None
        dimension_width = None
        dimension_unit_str = ""
        dimension_dict = {}

        asset_name = asset_dict.get("asset_name", "")
        # Ignore dimensions for Atlas Textures
        if (asset_name.lower()).startswith("atlas"):
            return None

        dimension_str = None
        render_custom_schema = asset_dict.get("render_custom_schema", None)
        if render_custom_schema is not None:
            dimension_str = render_custom_schema.get("dimensions", None)

            technical_desc = render_custom_schema.get("technical_description", {})
            if type(technical_desc) is dict:
                dimension_dict = technical_desc.get("Dimensions", {})
                if dimension_dict is None:
                    dimension_dict = {}

            for key in dimension_dict.keys():
                dimension_key = key.split(" ")
                dimension_unit_str = dimension_key[1]
                if "height" in key.lower():
                    dimension_height = float(dimension_dict[key])
                elif "width" in key.lower():
                    dimension_width = float(dimension_dict[key])

        if dimension_height is None and dimension_str is not None:
            dimension_val = dimension_str.split(" ")

            # Expected format for the dimension string is "2.5 x 2.5 m"
            try:
                dimension_height = float(dimension_val[0])
                dimension_width = float(dimension_val[2])
                dimension_unit_str = str(dimension_val[3])
            except ValueError:
                # if the first value is not integer, consider the format invalid
                return None

        if dimension_height is None and dimension_width is None:
            return None

        multiplier = 1
        if "cm" in dimension_unit_str:
            multiplier = 1
        elif "in" in dimension_unit_str:
            multiplier = 2.54
        elif "m" in dimension_unit_str:
            multiplier = 100

        return dimension_width * multiplier, dimension_height * multiplier

    @staticmethod
    def _decode_render_schema_tex(asset_dict: Dict
                                  ) -> Tuple[Dict[str, assets.TextureMapDesc],
                                             List[str],
                                             List[str]]:
        """Decodes render_schema from ApiResponse for
        Textures, HDRIs and Brushes

        Return value: Tuple[0] - Dictionary of TextureMapDesc indexed by workflow
                      Tuple[1] - List of all available sizes
                      Tuple[2] - List of all available variants
        """

        if "render_schema" not in asset_dict.keys():
            return ({}, [], [])

        all_sizes = []
        all_variants = []
        tex_desc_dict = {}  # {workflow: List[TextureMapDesc]
        for schema in asset_dict["render_schema"]:
            if "types" not in schema.keys():
                continue

            workflow = schema.get("name", "REGULAR")
            tex_descs = []
            for tex_type in schema.get("types", []):
                tex_code = tex_type.get("type_code", "")
                variant = None
                if "_" in tex_code:
                    tex_code, variant = tex_code.split("_")
                if tex_code not in assets.MAPS_TYPE_NAMES:
                    tex_code = assets.MapType.UNKNOWN.name
                map_type = assets.MapType[tex_code]

                if variant is not None:
                    variants = [variant]
                    all_variants.append(variant)
                else:
                    variants = []

                type_code = tex_type.get("type_code", "")
                type_name = tex_type.get("type_name", "")
                type_options = tex_type.get("type_options", [])
                type_preview = tex_type.get("type_preview", "")
                tex_desc = assets.TextureMapDesc(map_type_code=type_code,
                                                 display_name=type_name,
                                                 sizes=type_options,
                                                 filename_preview=type_preview,
                                                 variants=variants)
                tex_desc_variant = None
                if variant is None:
                    for tex_desc_prev in tex_descs:
                        if tex_desc_prev.get_map_type() == map_type:
                            tex_desc_variant = tex_desc_prev
                            break

                if tex_desc_variant is None:
                    tex_descs.append(tex_desc)
                else:
                    # TODO(Andreas): Currently assuming,
                    # variants are otherwise identical
                    tex_desc_variant.variants.extend(tex_desc.variants)

                all_sizes.extend(tex_desc.sizes)

            tex_desc_dict[workflow] = tex_descs

        # consolidate all sizes and variants for use in menus
        all_sizes = sorted(list(set(all_sizes)))
        all_variants = sorted(list(set(all_variants)))
        return (tex_desc_dict, all_sizes, all_variants)

    def _is_valid_size(self, size: str) -> bool:
        """Checks sizes coming from API to be in 'xK' format."""

        if size in [None, ""] or size[-1] != "K":
            return False
        try:
            int(size[:-1])
        except BaseException:
            msg = f"Failed to convert {size} to integer"
            self.capture_message("assetindex_size_conversion", msg)
            return False
        return True

    def _decode_render_schema_model(self,
                                    asset_dict: Dict
                                    ) -> Tuple[List[str], str]:
        """Decodes render_schema from ApiResponse for Models

        Returns a tuple:
        tuple[0] - List of all available sizes
        tuple[1] - Default size from included_resolution (if available)
        """

        if "render_schema" not in asset_dict.keys():
            msg = f"'render_schema' missing in asset dict\n{asset_dict}"
            self.capture_message("assetindex_no_renderschema", msg)
            return []
        render_schema = asset_dict.get("render_schema", {})
        if "options" not in render_schema.keys():
            msg = f"'options' missing in 'render_schema'\n{render_schema}"
            self.capture_message("assetindex_no_renderschema_options", msg)
            return []

        all_sizes = render_schema.get("options", [])
        all_sizes = [size for size in all_sizes if self._is_valid_size(size)]

        render_custom_schema = asset_dict.get("render_custom_schema", {})
        incl_size = None
        if "included_resolution" in render_custom_schema.keys():
            incl_size = render_custom_schema.get("included_resolution", "")
            if self._is_valid_size(incl_size):
                all_sizes.extend([incl_size])

        all_sizes = sorted(list(set(all_sizes)),
                           key=lambda s: int(s[:-1]))

        return all_sizes, incl_size

    def _construct_brush(self, asset_dict: Dict) -> assets.Brush:
        """Constructs a Brush"""

        tex = self._construct_texture(asset_dict)
        brush = assets.Brush(tex)
        return brush

    def _construct_model(self, asset_dict: Dict) -> assets.Model:
        """Constructs a Model"""

        model = assets.Model()
        if "lods" in asset_dict.keys():
            model.lods = asset_dict["lods"]
        model.sizes, model.size_default = self._decode_render_schema_model(
            asset_dict)
        return model

    def _construct_hdri(self, asset_dict: Dict) -> assets.Hdri:
        """Constructs an HDRI"""

        tex_info = self._decode_render_schema_tex(asset_dict)

        tex_map_descs = tex_info[0]

        if "REGULAR" not in tex_map_descs:
            msg = f"HDRI without REGULAR workflow'\n{asset_dict}"
            self.capture_message("assetindex_hdri_not_regular", msg)
            raise KeyError("HDRI and no REGULAR workflow")

        tex_map_descs_bg = {}
        tex_map_descs_light = {}
        for workflow, tex_map_desc_list in tex_map_descs.items():
            for tex_desc in tex_map_desc_list:
                if tex_desc.get_map_type() == assets.MapType.JPG:
                    tex_map_descs_bg[workflow] = [tex_desc]
                elif tex_desc.get_map_type() == assets.MapType.HDR:
                    tex_map_descs_light[workflow] = [tex_desc]
                else:
                    msg = f"HDRI with unexpected texture map type: {tex_desc.map_type_code}"
                    self.capture_message("assetindex_hdri_map_type", msg)
                    raise ValueError(msg)

        tex_bg = assets.Texture(map_descs=tex_map_descs_bg,
                                sizes=tex_info[1],
                                variants=tex_info[2])
        tex_bg.watermarked_urls = self._filter_image_urls(
            asset_dict["toolbox_previews"])
        tex_bg.maps = {}

        tex_light = assets.Texture(map_descs=tex_map_descs_light,
                                   sizes=tex_info[1],
                                   variants=tex_info[2])
        tex_light.watermarked_urls = tex_bg.watermarked_urls
        tex_light.maps = {}

        hdri = assets.Hdri(tex_bg, tex_light)
        return hdri

    def _construct_texture(self, asset_dict: Dict) -> assets.Texture:
        """Constructs a Texture"""

        tex_info = self._decode_render_schema_tex(asset_dict)
        tex_rw_dimension = self._get_texture_real_world_dimension(asset_dict)

        tex = assets.Texture(map_descs=tex_info[0],
                             sizes=tex_info[1],
                             variants=tex_info[2],
                             real_world_dimension=tex_rw_dimension)
        tex.watermarked_urls = self._filter_image_urls(
            asset_dict["toolbox_previews"])
        tex.maps = {}
        return tex

    def _construct_asset_base(self,
                              asset_dict: Dict,
                              purchased: Optional[bool] = None
                              ) -> assets.AssetData:
        """Constructs AssetData part common to all types"""

        asset_type = assets.API_TYPE_TO_ASSET_TYPE[asset_dict["type"]]
        if asset_type == assets.AssetType.SUBSTANCE:
            raise NotImplementedError("Substances not supported, yet")

        asset_data = assets.AssetData(asset_id=asset_dict["id"],
                                      asset_type=asset_type,
                                      asset_name=asset_dict["asset_name"])
        asset_data.display_name = asset_dict["name"]
        asset_data.categories = []
        for category in asset_dict["categories"]:
            category = category.title()
            if category in assets.CATEGORY_TRANSLATION:
                category = assets.CATEGORY_TRANSLATION[category]
            asset_data.categories.append(category)
        asset_data.url = asset_dict["url"]
        asset_data.slug = asset_dict["slug"]
        asset_data.credits = asset_dict["credit"]
        asset_data.thumb_urls = self._filter_image_urls(asset_dict["previews"])
        published_at = asset_dict["published_at"]
        t_published_at = time.strptime(published_at, "%Y-%m-%d %H:%M:%S")
        seconds_since_epoch = time.mktime(t_published_at)
        asset_data.published_at = seconds_since_epoch  # TODO(Andreas): need to take timezone into account
        asset_data.is_local = None
        asset_data.downloaded_at = None
        asset_data.is_purchased = purchased
        asset_data.purchased_at = None
        asset_data.render_custom_schema = asset_dict.get(
            "render_custom_schema", {})
        return asset_data

    def construct_asset(self,
                        asset_dict: Dict,
                        purchased: Optional[bool] = None
                        ) -> assets.AssetData:
        """Constructs an AssetData from an asset dictionary
        as found in ApiResponse"""

        try:
            asset_data = self._construct_asset_base(asset_dict, purchased)
            asset_type = asset_data.asset_type
            if asset_type == assets.AssetType.BRUSH:
                asset_data.brush = self._construct_brush(asset_dict)
            elif asset_type == assets.AssetType.HDRI:
                asset_data.hdri = self._construct_hdri(asset_dict)
            elif asset_type == assets.AssetType.MODEL:
                asset_data.model = self._construct_model(asset_dict)
            elif asset_type == assets.AssetType.TEXTURE:
                asset_data.texture = self._construct_texture(asset_dict)
        except NotImplementedError:
            raise  # forward Substance exception
        return asset_data

    def update_asset(self,
                     asset_id: int,
                     asset_data_new: assets.AssetData,
                     purge_maps: bool = False) -> None:
        """Updates an AssetData entry with information
        found in asset_data_new.

        NOTE: Any non-None entry will _overwrite_ the old one.
        """

        if asset_id not in self.all_assets:
            return

        asset_data = self.all_assets[asset_id]
        # Some members are not meant to be updated:
        # Namely: asset_id, asset_type, asset_name
        if asset_id != asset_data_new.asset_id:
            msg = (f"Cannot change asset ID ({asset_id} to "
                   f"{asset_data_new.asset_id})!")
            self.capture_message("assetindex_update_id_mismatch", msg)
            raise ValueError(msg)
        if asset_data.asset_name != asset_data_new.asset_name:
            msg = (f"Cannot change asset name ({asset_data.asset_name} to "
                   f"{asset_data_new.asset_name})!")
            self.capture_message("assetindex_update_name_mismatch", msg)
            raise ValueError(msg)
        if asset_data.asset_type != asset_data_new.asset_type:
            msg = (f"Cannot change asset type ({asset_data.asset_type} to "
                   f"{asset_data_new.asset_type})!")
            self.capture_message("assetindex_update_type_mismatch", msg)
            raise ValueError(msg)
        asset_data.update(asset_data_new, purge_maps)

    def mark_purchased(self, asset_id: int) -> None:
        """Marks an AssetData as purchased"""

        if asset_id not in self.all_assets:
            return
        self.all_assets[asset_id].is_purchased = True
        utc_s_since_epoch = datetime.now(timezone.utc).timestamp()
        self.all_assets[asset_id].purchased_at = utc_s_since_epoch

    def _map_type_from_filename_parts(self, filename_parts: List[str]):
        """Gets a MapType (and its workflow) from a list of parts of a filename.

        Args:
        filename_parts: List with strings containing different sections
                        of a filename
        """

        map_type_name = None
        for filename_part in filename_parts:
            if filename_part in assets.MAPS_TYPE_NAMES:
                map_type_name = filename_part
                break
        # For example backdrops differ in naming convention and do not contain
        # a map type in their filename. In this case image files are classified
        # as diffuse.
        if map_type_name is None:
            map_type_name = "DIFF"
        workflow = None
        for filename_part in filename_parts:
            if filename_part in assets.WORKFLOWS:
                workflow = filename_part
                break

        if map_type_name is not None:
            map_type = assets.MapType[map_type_name]
        else:
            map_type = None
        return map_type, workflow

    def _lod_from_filename_parts(self, filename_parts: List[str]):
        """Gets the LOD (string) from a list of parts of a filename.

        Args:
        filename_parts: List with strings containing different sections
                        of a filename
        """

        lods = [lod for lod in assets.LODS if lod in filename_parts]
        num_lods = len(lods)

        if num_lods > 0:
            lod = lods[0]
            if num_lods > 1:
                msg_warn = ("One fbx, multiple lods?\n"
                            f"filename_parts: {filename_parts[0]}\n"
                            f"lods: {lods}")
                self.logger.warning(msg_warn)
        else:
            lod = None
        return lod

    def _size_from_filename_parts(self, filename_parts: List[str]):
        """Gets the size (string) from a list of parts of a filename.

        Args:
        filename_parts: List with strings containing different sections
                        of a filename
        """

        sizes = [size for size in assets.SIZES if size in filename_parts]
        num_sizes = len(sizes)

        if num_sizes > 0:
            size = sizes[0]
            if num_sizes > 1:
                msg_warn = ("One file, multiple sizes?\n"
                            f"filename_parts: {filename_parts[0]}\n"
                            f"sizes: {sizes}")
                self.logger.warning(msg_warn)
        else:
            size = None
        return size

    def _variant_from_filename_parts(self, filename_parts: List[str]):
        """Gets the variant (string) from a list of parts of a filename.

        Args:
        filename_parts: List with strings containing different sections
                        of a filename
        """

        variants = [
            variant for variant in assets.VARIANTS
            if variant in filename_parts
        ]
        num_variants = len(variants)

        if num_variants > 0:
            variant = variants[0]
            if num_variants > 1:
                msg_warn = ("One file, multiple variants?\n"
                            f"filename_parts: {filename_parts[0]}\n"
                            f"variants: {variants}")
                self.logger.warning(msg_warn)
        else:
            variant = None
        return variant

    def _analyze_single_file(self,
                             path: str,
                             filename: str,
                             workflow_fallback: str,
                             lods: List[str],
                             sizes: List[str],
                             variants: List[str],
                             previews: List[str],
                             texture_maps: List[assets.TextureMap],
                             meshes: List[assets.ModelMesh]
                             ) -> None:
        base_filename, suffix = os.path.splitext(filename)
        base_filename_low = base_filename.lower()
        suffix = suffix.lower()

        if any(base_filename_low.endswith(preview_name) for preview_name in assets.PREVIEWS):
            previews.append(filename)
            return

        if "_SOURCE" in base_filename:
            return

        name_parts = base_filename.split("_")  # do not use base_filename_low, here
        if suffix in [".jpg", ".jpeg", ".png", ".tif", ".exr", ".psd"]:
            map_type, workflow_file = self._map_type_from_filename_parts(name_parts)
        else:
            map_type = None
            workflow_file = None
        lod = self._lod_from_filename_parts(name_parts)
        size = self._size_from_filename_parts(name_parts)
        variant = self._variant_from_filename_parts(name_parts)

        if workflow_file is None:
            workflow_file = workflow_fallback
        if lod is not None:
            lods.append(lod)
        else:
            lods.append("NONE")
            lod = "NONE"
        if size is not None:
            sizes.append(size)
        if variant is not None:
            variants.append(variant)
        if map_type is not None:
            tex_map = assets.TextureMap(map_type=map_type,
                                        size=size,
                                        variant=variant,
                                        lod=lod,
                                        filename=filename,
                                        directory=path)
            if workflow_file in texture_maps:
                texture_maps[workflow_file].append(tex_map)
            else:
                texture_maps[workflow_file] = [tex_map]
        elif suffix == ".fbx":
            mesh = assets.ModelMesh(model_type=assets.ModelType.FBX,
                                    lod=lod,
                                    filename=filename,
                                    directory=path)
            meshes.append(mesh)
        elif suffix == ".blend":
            mesh = assets.ModelMesh(model_type=assets.ModelType.BLEND,
                                    lod=lod,
                                    filename=filename,
                                    directory=path)
            meshes.append(mesh)
        elif suffix == ".c4d":
            mesh = assets.ModelMesh(model_type=assets.ModelType.C4D,
                                    lod=lod,
                                    filename=filename,
                                    directory=path)
            meshes.append(mesh)
        elif suffix == ".max":
            mesh = assets.ModelMesh(model_type=assets.ModelType.MAX,
                                    lod=lod,
                                    filename=filename,
                                    directory=path)
            meshes.append(mesh)
        elif suffix.endswith("dl"):
            # a temporary file from a cancelled download
            pass
        else:
            # TODO(Andreas): Is there anything we want to do with
            #                unexpected files?
            self.logger.info(f"Unexpected file type: {filename}")

    def _analyze_files(self,
                       dir_asset: str,
                       workflow_fallback: str,
                       lods: List[str],
                       sizes: List[str],
                       variants: List[str],
                       previews: List[str],
                       texture_maps: List[assets.TextureMap],
                       meshes: List[assets.ModelMesh]
                       ) -> None:
        """Analyzes files in a directory and fills the passed in lists
        with the information found.
        """

        for path, dirs, files in os.walk(dir_asset):
            files = sorted(list(set(files)))
            for file in files:
                self._analyze_single_file(path,
                                          file,
                                          workflow_fallback,
                                          lods,
                                          sizes,
                                          variants,
                                          previews,
                                          texture_maps,
                                          meshes)

    @staticmethod
    def check_if_preview(filename: str, only_anim: bool = False) -> bool:
        """Determines if the target filename is a recognized preview file."""
        base_filename, suffix = os.path.splitext(filename)
        base_filename_low = base_filename.lower()
        suffix = suffix.lower()

        if suffix == ".mp4":
            # Non image preview
            return True if only_anim else False
        if only_anim is True:
            return False  # Short circuit for not being an animated file.
        if suffix not in assets.PREVIEW_EXTS_LOWER:
            # Skip anything that isn't an image format.
            return False
        elif any(base_filename_low.endswith(preview_name) for preview_name in assets.PREVIEWS):
            return True
        elif _PREVIEW_PATTERN.search(base_filename_low):
            # General check for any _preview, _preview1, _preview01, _previewA
            return True
        else:
            # Everything else, ie material map passes
            return False

    def _prepare_brush_update_asset_data(self,
                                         workflow: str,
                                         texture_maps: List[assets.TextureMap],
                                         asset_data_update: assets.AssetData
                                         ) -> None:
        alpha = assets.Texture()
        brush = assets.Brush(alpha)
        alpha.maps = {workflow: []}
        if workflow not in texture_maps:
            return False
        files_found = False
        for tex_map in texture_maps[workflow]:
            is_alpha = tex_map.map_type == assets.MapType.ALPHA
            if not is_alpha:
                continue
            alpha.maps[workflow].append(tex_map)
            files_found = True
        asset_data_update.brush = brush
        return files_found

    def _prepare_hdri_update_asset_data(self,
                                        workflow: str,
                                        texture_maps: List[assets.TextureMap],
                                        asset_data_update: assets.AssetData
                                        ) -> None:
        bg = assets.Texture()
        light = assets.Texture()
        hdri = assets.Hdri(bg, light)
        bg.maps = {workflow: []}
        light.maps = {workflow: []}
        if workflow not in texture_maps:
            return False
        files_found = False
        for tex_map in texture_maps[workflow]:
            is_bg = tex_map.map_type == assets.MapType.ENV
            is_bg |= tex_map.map_type == assets.MapType.JPG
            is_light = tex_map.map_type == assets.MapType.LIGHT
            is_light |= tex_map.map_type == assets.MapType.HDR
            if not is_bg and not is_light:
                continue
            elif is_bg:
                bg.maps[workflow].append(tex_map)
                files_found = True
            elif is_light:
                light.maps[workflow].append(tex_map)
                files_found = True
        asset_data_update.hdri = hdri
        return files_found

    def _prepare_model_update_asset_data(self,
                                         workflow: str,
                                         meshes: List[assets.ModelMesh],
                                         texture_maps: List[assets.TextureMap],
                                         sizes: List[str],
                                         variants: List[str],
                                         lods: List[str],
                                         asset_data_update: assets.AssetData
                                         ) -> bool:

        files_found = len(texture_maps) > 0
        if not files_found:
            return False

        tex = assets.Texture()

        map_descs = {}
        for workflow_tex, tex_map_list in texture_maps.items():
            for tex_map in tex_map_list:
                tex_map_desc = assets.TextureMapDesc(display_name="",
                                                     filename_preview="",
                                                     map_type_code=tex_map.map_type.name,
                                                     sizes=sizes,
                                                     variants=variants)
                if workflow_tex not in map_descs:
                    map_descs[workflow_tex] = []
                if tex_map_desc not in map_descs[workflow_tex]:
                    map_descs[workflow_tex].append(tex_map_desc)

        tex.map_descs = map_descs
        tex.maps = texture_maps
        tex.sizes = sizes
        tex.variants = variants
        tex.lods = lods

        model = assets.Model()
        model.meshes = meshes
        model.texture = tex
        model.lods = lods
        # Do not touch sizes, it contains ALL sizes from query
        #   and WM maps are not supported by Model assets
        #   (so we can not find an additional size)
        # model.sizes = sizes
        model.variants = variants

        asset_data_update.model = model
        return files_found

    def _prepare_tex_update_asset_data(self,
                                       workflow: str,
                                       texture_maps: List[assets.TextureMap],
                                       sizes: List[str],
                                       asset_data_update: assets.AssetData
                                       ) -> None:
        tex = assets.Texture()
        tex.maps = texture_maps
        tex.sizes = sizes
        asset_data_update.texture = tex

        files_found = len(texture_maps) > 0
        # If only maps of size WM got found, the asset will not be regarded local
        if sizes == ["WM"]:
            files_found = False
        return files_found

    def update_from_directory(self,
                              asset_id: int,
                              dir_asset: str,
                              workflow_fallback: str = "REGULAR"
                              ) -> bool:
        """Store texture file references into Textures, HDRIs and Brushes.

        Args:
        asset_id: ID of the asset to update
        dir_asset: The directory to search for new files
        workflow_fallback: Used in case, there is no workflow found in a filename

        Return value:
        True, if files were found, otherwise False
        """

        if asset_id not in self.all_assets:
            msg = f"Unable to update, asset_id {asset_id} not found"
            self.capture_message("assetindex_update_id_missing", msg)
            raise KeyError(msg)

        asset_data = self.all_assets[asset_id]
        asset_name = asset_data.asset_name
        asset_type = asset_data.asset_type
        asset_type_data = asset_data.get_type_data()
        workflow = asset_type_data.get_workflow(workflow_fallback)
        if workflow is None:
            workflow = workflow_fallback

        lods = []
        sizes = []
        variants = []
        previews = []
        texture_maps = {}
        meshes = []
        self._analyze_files(dir_asset,
                            workflow,
                            lods,
                            sizes,
                            variants,
                            previews,
                            texture_maps,
                            meshes)

        lods = sorted(list(set(lods)))
        sizes = sorted(list(set(sizes)))
        variants = sorted(list(set(variants)))

        # append files to asset_data
        asset_data_update = assets.AssetData(asset_id, asset_type, asset_name)
        if asset_type == assets.AssetType.BRUSH:
            files_found = self._prepare_brush_update_asset_data(workflow_fallback,
                                                                texture_maps,
                                                                asset_data_update)
        elif asset_type == assets.AssetType.HDRI:
            files_found = self._prepare_hdri_update_asset_data(workflow_fallback,
                                                               texture_maps,
                                                               asset_data_update)
        elif asset_type == assets.AssetType.MODEL:
            files_found = self._prepare_model_update_asset_data(workflow_fallback,
                                                                meshes,
                                                                texture_maps,
                                                                sizes,
                                                                variants,
                                                                lods,
                                                                asset_data_update)
        elif asset_type == assets.AssetType.TEXTURE:
            files_found = self._prepare_tex_update_asset_data(workflow_fallback,
                                                              texture_maps,
                                                              sizes,
                                                              asset_data_update)

        self.update_asset(asset_id, asset_data_update)

        if files_found:
            utc_s_since_epoch = datetime.now(timezone.utc).timestamp()
            asset_data.downloaded_at = utc_s_since_epoch
            asset_data.is_local = True
        return files_found

    def update_all_local_assets(self,
                                library_dirs: List[str],
                                workflow_fallback: str = "REGULAR",
                                purchased: Optional[bool] = True
                                ) -> Tuple[Dict[str, int], List[str]]:
        """Updates "locality" of assets from one or more of library directories.

        AssetIndex needs to be populated beforehand
        with a my_assets query.

        Order in library_dirs matters. By convention, the first directory
        should be the "primary" directory.

        Args:
        library_dirs: List of library directories. Lower index directories win.
                      E.g. if a texture file is found in 1st and 3rd library
                      directory, the TextureMap will point to the one in the
                      1st directory.
        workflow_fallback: Used in case there is no workflow found in filename
        purchased: By default (purchased=True) only purchased assets will be
                   updated. Set to None to update _all_ assets and False to
                   only update non-purchased assets.

        Return value:
        A tuple with a dictionary and a list:
        tuple[0]: Dict {asset name: asset ID} with assets no files were found for.
        tuple[1]: Contains directories no matching asset was found for.
        """

        # Gather list with names of purchased assets
        asset_id_list = self.get_asset_id_list(
            asset_type=None, purchased=purchased)
        asset_name_dict = {
            self.all_assets[asset_id].asset_name: asset_id
            for asset_id in asset_id_list
        }

        # update_from_directory() overwrites file reference entries.
        # Thus the primary library directory has to be scanned last.
        library_dirs = reversed(library_dirs)

        for _asset_id in asset_id_list:
            asset_data = self.get_asset(_asset_id)
            asset_data.is_local = False

        # Browse library_dirs recursively
        matched_assets = []
        unmatched_directories = []
        for dir_library in library_dirs:
            for path, dirs, files in os.walk(dir_library):
                if len(dirs) == 0:
                    continue
                for directory in dirs:
                    # Match _directory_ names with list of purchased assets
                    dir_asset = os.path.join(path, directory)
                    if directory in asset_name_dict.keys():
                        asset_id = asset_name_dict[directory]
                        files_found = self.update_from_directory(
                            asset_id,
                            dir_asset,
                            workflow_fallback
                        )
                        if files_found and directory not in matched_assets:
                            matched_assets.append(directory)
                    elif dir_asset != dir_library:
                        unmatched_directories.append(dir_asset)

        for asset_name in matched_assets:
            try:
                del asset_name_dict[asset_name]
            except KeyError as e:
                self.capture_message("assetindex_update_local_key_error", e)

        return asset_name_dict, unmatched_directories

    def load_asset(
            self, asset_data: assets.AssetData, replace: bool = False) -> None:
        """Stores or updates an AssetData in cache"""

        asset_id = asset_data.asset_id
        if asset_id not in self.all_assets:
            self.all_assets[asset_id] = asset_data
        else:
            self.update_asset(asset_id, asset_data)

    # TODO(SOFT-598): Have it in its own class
    def _query_key_to_tuple(self,
                            key_query: str,
                            chunk: Optional[int],
                            chunk_size: Optional[int],
                            ):
        # key_query format: "tab/type[/category[/search]]"
        if "/" in key_query:
            query_parts = key_query.split("/")
            has_search = False
        elif "@" in key_query:
            query_parts = key_query.split("@")
            has_search = True
        else:
            msg = f"Unknown query format: {key_query}"
            self.capture_message("assetindex_query_unknown", msg)
            raise ValueError(msg)
        # Query tab, one of: poliigon, my_assets, imported
        query_tab = query_parts[0]
        # Query type, one of: All Assets, Brushes, HDRIs, Models, Textures
        query_type = assets.CATEGORY_NAME_TO_ASSET_TYPE[query_parts[1]]

        query_category = None
        query_search = None
        if has_search:
            query_search = query_parts[-1].lower()
            if len(query_parts) > 2:
                query_category = "/".join(query_parts[2:-1])
        else:
            if len(query_parts) > 2:
                query_category = "/".join(query_parts[2:])

        query_tuple = (query_tab,
                       query_type,
                       query_category,
                       query_search,
                       chunk,
                       chunk_size)
        return query_tuple

    def populate_assets(self,
                        resp: api.ApiResponse,
                        key_query: str,
                        chunk: Optional[int] = None,
                        chunk_size: Optional[int] = None,
                        append_query=False
                        ) -> List[int]:
        """Populates cache from an ApiResponse.

        Args:
        resp: The ApiResponse after querying the server.
        key_query: Query string, format: tab/type/category/search
        chunk: Index of a given chunk, e.g. page index. Uses page index from
               resp, if chunk is None.
        chunk_size: Size of the chunk, e.g. page size. Uses page size from
                    resp, if chunk_size is None.
        append_query: If False, the query will be replaced in query cache.
                      If True, the assets get appended to the query in cache.

        NOTE: AssetIndex uses chunk and chunksize from ApiResponse.
        """

        if not resp.ok:
            return
        try:
            asset_dict_list = resp.body["data"]
        except KeyError:
            msg = "Lacking `data` in ApiResponse body"
            self.logger.exception(msg)
            self.capture_message("assetindex_populate_no_data", msg)
            return []
        except BaseException as e:
            self.logger.exception("Unexpected Exception")
            self.capture_message("assetindex_unexpected",
                                 f"Unexpected Exception: {e}")
            raise

        if chunk is None:
            chunk = resp.body.get("current_page", -1)
        if chunk_size is None:
            chunk_size = resp.body.get("per_page", -1)

        query_tuple = self._query_key_to_tuple(key_query, chunk, chunk_size)

        if not append_query or query_tuple not in self.cached_queries:
            self.cached_queries[query_tuple] = []
        # purchased is either True or None (NOT False),
        # we must NOT overwrite a purchased state with False.
        purchased = True if query_tuple[0] == "my_assets" else None
        tmp_cached_query = []
        for asset_dict in asset_dict_list:
            try:
                asset_data = self.construct_asset(asset_dict, purchased)
                self.load_asset(asset_data)  # deals with fresh insert vs patch update
                tmp_cached_query.append(asset_data.asset_id)
            except NotImplementedError:
                pass  # silence Substance exceptions
        self.cached_queries[query_tuple].extend(tmp_cached_query)
        return tmp_cached_query

    def filter_asset_ids_by_category(self, asset_id_list, category):
        if category is None:
            return asset_id_list

        asset_id_list_filtered = []
        for asset_id in asset_id_list:
            if category in self.all_assets[asset_id].categories:
                asset_id_list_filtered.append(asset_id)
        return asset_id_list_filtered

    def filter_asset_ids_by_search(self, asset_id_list, search):
        if search is None:
            return asset_id_list

        asset_id_list_filtered = []
        for asset_id in asset_id_list:
            if search in self.all_assets[asset_id].asset_name.lower():
                asset_id_list_filtered.append(asset_id)
        return asset_id_list_filtered

    def filter_asset_ids_by_credits(self, asset_id_list, credits):
        if credits is None:
            return asset_id_list

        asset_id_list_filtered = []
        for asset_id in asset_id_list:
            if credits <= self.all_assets[asset_id].credits:
                asset_id_list_filtered.append(asset_id)
        return asset_id_list_filtered

    def query(self,
              key_query: str,
              chunk: Optional[int],
              chunk_size: Optional[int],
              fail_on_miss: bool = True
              ) -> Optional[List[int]]:
        """Returns a list of asset IDs based on key_query. Query gets cached.

        Args:
        key_query: Query string, format: tab/type/category/search
        chunk: May represent a UI page number or any other kind of segment index
        chunk_size: The maximum number of assets in a chunk (aka page or segment)
        fail_on_miss: If True, query() will return None, if key_query is not
                      found in query cache.
                      False is not implemeted, yet. In this case query() will
                      perform an offline search of its contents.
        """

        query_tuple = self._query_key_to_tuple(key_query, chunk, chunk_size)

        if query_tuple in self.cached_queries:
            return self.cached_queries[query_tuple]
        elif fail_on_miss:
            return None  # subtle hint to request data from server

        # Answer query from AssetIndex content
        asset_id_list = self.get_asset_id_list(query_tuple[1])
        asset_id_list = self.filter_asset_ids_by_category(asset_id_list,
                                                          query_tuple[2])
        asset_id_list_search = self.filter_asset_ids_by_search(asset_id_list,
                                                               query_tuple[3])
        if query_tuple[3] == "free":
            asset_id_list_credits = self.filter_asset_ids_by_credits(
                asset_id_list, 0)
            asset_id_list = asset_id_list_search + asset_id_list_credits
            asset_id_list = list(set(asset_id_list))
        else:
            asset_id_list = asset_id_list_search
        return asset_id_list

    def store_query(self,
                    asset_ids: List[int],
                    key_query: str,
                    chunk: Optional[int] = None,
                    chunk_size: Optional[int] = None
                    ) -> None:
        """Stores a list of asset IDs in query cache."""

        query_tuple = self._query_key_to_tuple(key_query, chunk, chunk_size)
        self.cached_queries[query_tuple] = asset_ids

    def query_exists(self,
                     key_query: str,
                     chunk: Optional[int] = None,
                     chunk_size: Optional[int] = None
                     ) -> bool:
        """Returns True if query in cache."""

        query_tuple = self._query_key_to_tuple(key_query, chunk, chunk_size)
        return query_tuple in self.cached_queries

    def sort_query(self,
                   key_query: str = "My Assets",
                   key_field: str = "",
                   reverse: bool = False) -> List[int]:
        """Returns a sorted list of asset IDs by sorting a cached query.
        Will implicitly call query, if query not in cache.
        Query cache will be updated with the sorted list in the process.
        """

        return []

    def get_asset(self, asset_id: int) -> Optional[assets.AssetData]:
        """Returns the AssetData belonging to an asset ID"""

        if asset_id not in self.all_assets:
            return None
        return self.all_assets[asset_id]

    def get_asset_data_list(
            self, asset_ids: List[int]) -> List[assets.AssetData]:
        """Returns list of AssetData belonging to a list of asset IDs"""

        assets = []
        for asset_id in asset_ids:
            if asset_id in self.all_assets:
                assets.append(self.all_assets[asset_id])
            else:
                assets.append(None)
        return assets

    def get_asset_name(self,
                       asset_id: int,
                       beauty: bool = False) -> Optional[str]:
        """Gets name of a specific asset."""

        if asset_id not in self.all_assets:
            return None
        if beauty:
            name = self.all_assets[asset_id].display_name
        else:
            name = self.all_assets[asset_id].asset_name
        return name

    def get_asset_workflow_list(self, asset_id: int) -> Optional[List[str]]:
        """Gets list of workflows for a specific asset."""

        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        type_data = asset_data.get_type_data()
        return type_data.get_workflow_list()

    def get_asset_workflow(self,
                           asset_id: int,
                           workflow: str = "REGULAR") -> Optional[str]:
        """Verifies a workflow for a specific asset."""

        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        type_data = asset_data.get_type_data()
        return type_data.get_workflow(workflow)

    def get_asset_size_list(self,
                            asset_id: int,
                            incl_watermarked: bool = False,
                            local_only: bool = False) -> Optional[List[str]]:
        """Gets list of sizes/lods for a specific asset."""

        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        type_data = asset_data.get_type_data()
        return type_data.get_size_list(incl_watermarked, local_only)

    def get_asset_size(self,
                       asset_id: int,
                       size: str = "1K") -> Optional[List[str]]:
        """Verifies size for a specific asset."""

        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        type_data = asset_data.get_type_data()
        return type_data.get_size(size)

    def get_asset_variant_list(self, asset_id: int) -> Optional[List[str]]:
        """Gets list of variants for a specific asset."""

        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        type_data = asset_data.get_type_data()
        return type_data.get_variant_list()

    def get_asset_lod_list(self, asset_id: int) -> Optional[List[str]]:
        """Gets list of lods for a specific asset."""

        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        if asset_data.asset_type != assets.AssetType.MODEL:
            return None
        type_data = asset_data.get_type_data()
        return type_data.get_lod_list()

    def get_asset_lod(self, asset_id: int, lod: str = "LOD1") -> Optional[str]:
        """Gets list of lods for a specific asset."""

        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        if asset_data.asset_type != assets.AssetType.MODEL:
            return None
        type_data = asset_data.get_type_data()
        return type_data.get_lod(lod)

    def get_asset_map_type_list(self,
                                asset_id: int,
                                workflow: str,
                                prefer_16_bit: bool = False
                                ) -> Optional[List[assets.MapType]]:
        """Gets list of MapType belonging to a given workflow"""

        if asset_id not in self.all_assets:
            return []
        asset_data = self.all_assets[asset_id]

        if asset_data.asset_type == assets.AssetType.SUBSTANCE:
            raise NotImplementedError(
                "Asset type SUBSTANCE not supported, yet")

        map_types = asset_data.get_type_data().get_map_type_list(workflow)

        has_bump = assets.MapType.BUMP in map_types
        has_bump16 = assets.MapType.BUMP16 in map_types
        if has_bump and has_bump16:
            if prefer_16_bit:
                map_types.remove(assets.MapType.BUMP)
            else:
                map_types.remove(assets.MapType.BUMP16)

        has_disp = assets.MapType.DISP in map_types
        has_disp16 = assets.MapType.DISP16 in map_types
        if has_disp and has_disp16:
            if prefer_16_bit:
                map_types.remove(assets.MapType.DISP)
            else:
                map_types.remove(assets.MapType.DISP16)

        has_normal = assets.MapType.NRM in map_types
        has_normal16 = assets.MapType.NRM16 in map_types
        if has_normal and has_normal16:
            if prefer_16_bit:
                map_types.remove(assets.MapType.NRM)
            else:
                map_types.remove(assets.MapType.NRM16)

        return map_types

    def check_asset_is_backtype(self, asset_data: assets.AssetData) -> bool:
        """Checks if this asset is a backplate or backdrop."""
        lower_name = asset_data.asset_name.lower()
        return "backdrop" in lower_name or "backplate" in lower_name

    def check_asset_is_local(self,
                             asset_id: int,
                             workflow: Optional[str] = None,
                             size: Optional[str] = None,
                             lod: Optional[str] = None,
                             model_type: Optional[assets.ModelType] = None,
                             native_only: bool = False
                             ) -> bool:
        """Checks if an asset (or a flavor thereof) has been downloaded.

        Args:
        asset_id: ID of the asset to check.
        workflow: Specify a workflow or None to check for any workflow.
        size: Specify a texture size or None to check for any size.
        lod: Specify a LOD or None to check for any LOD.
        """

        try:
            asset_data = self.all_assets[asset_id]
        except KeyError:
            msg = f"Asset ID {asset_id} not in AssetIndex"
            self.capture_message("assetindex_asset_missing", msg)
            return False

        if asset_data.asset_type == assets.AssetType.MODEL:
            asset_type_data = asset_data.get_type_data()
            mesh_is_local = asset_type_data.has_mesh(model_type, native_only)
        else:
            mesh_is_local = True

        if workflow is None and size is None and lod is None and model_type is None:
            return asset_data.is_local

        incl_watermarked = size == "WM"

        local_sizes = self.check_asset_local_sizes(
            asset_id, workflow, incl_watermarked)
        if size is None:
            tex_is_local = any(local_sizes.values())
        else:
            tex_is_local = size in local_sizes and local_sizes[size]

        if asset_data.asset_type != assets.AssetType.MODEL:
            return tex_is_local

        local_lods = self.check_asset_local_lods(
            asset_id, model_type, native_only)
        if lod is None:
            lod_is_local = any(local_lods.values())
        else:
            lod_is_local = lod in local_lods and local_lods[lod]

        return tex_is_local and lod_is_local and mesh_is_local

    def get_local_assets(self,
                         asset_type: Optional[assets.AssetType] = None,
                         workflow: Optional[str] = None,
                         size: Optional[str] = None,
                         ) -> List[assets.AssetData]:
        """Get a list of all (or just of a certain flavor) downloaded assets.

        Args:
        asset_type: Specify an AssetType or None for assets of any type.
        workflow: Specify a workflow or None for assets with any workflow.
        size: Specify a texture size or None for assets with textures of any size.
        """

        return []

    def check_asset_local_sizes(self,
                                asset_id: int,
                                workflow: Optional[str] = "REGULAR",
                                incl_watermarked: bool = False
                                ) -> Dict[str, bool]:
        """Returns texture 'locality' by size.

        Args:
        asset_id: ID of the asset to check.
        workflow: Workflow to check for. None for any workflow.

        Return value:
        Dict {size: is_local}
        """

        if asset_id not in self.all_assets:
            return {}

        asset_data = self.all_assets[asset_id]
        type_data = asset_data.get_type_data()

        local_sizes = {}
        all_sizes = type_data.get_size_list(incl_watermarked)
        for size in all_sizes:
            if workflow is None:
                workflow_list = self.get_asset_workflow_list(asset_id)
                local_sizes[size] = False
                for workflow_check in workflow_list:
                    maps = type_data.get_maps(workflow_check, size)
                    local_sizes[size] |= len(maps) != 0
            else:
                maps = type_data.get_maps(workflow, size)
                # TODO(Andreas): Here one could determine/check "completeness"
                local_sizes[size] = len(maps) != 0

        return local_sizes

    def check_asset_local_lods(self,
                               asset_id: int,
                               model_type: Optional[assets.ModelType] = None,
                               native_only: bool = False
                               ) -> Dict[str, bool]:
        """Returns model/mesh 'locality' by LOD.

        Args:
        asset_id: ID of the asset to check.

        Return value:
        Dict {lod: is_local}
        """

        if asset_id not in self.all_assets:
            return {}

        asset_data = self.all_assets[asset_id]

        if asset_data.asset_type != assets.AssetType.MODEL:
            return {}

        model_data = asset_data.get_type_data()

        local_lods = {}
        all_lods = model_data.get_lod_list()
        if all_lods is None:
            return {}

        for lod in all_lods:
            meshes_fbx = model_data.get_mesh(
                lod, model_type=assets.ModelType.FBX)
            if model_type is not None:
                meshes_native = model_data.get_mesh(lod, model_type=model_type)
            else:
                meshes_native = []

            if native_only:
                lod_is_local = len(meshes_native) > 0
            else:
                lod_is_local = len(meshes_fbx) > 0 or len(meshes_native) > 0

            # TODO(Andreas): Here one could determine/check "completeness"
            local_lods[lod] = lod_is_local

        return local_lods

    def get_thumbnail_url_list(self, asset_id: int) -> List[str]:
        """Gets _all_ URLs for an asset's thumbnails"""

        if asset_id not in self.all_assets:
            return None
        ad = self.all_assets[asset_id]
        return ad.thumb_urls

    def get_thumbnail_url_by_index(self,
                                   asset_id: int,
                                   index: int = 0) -> Optional[str]:
        """Returns preview url via index, if index exists,
        otherwise the first preview url will be returned.

        Return value may be None, e.g. in case of dummy entries.
        """

        if index < 0:
            raise ValueError
        if asset_id not in self.all_assets:
            return None
        ad = self.all_assets[asset_id]
        if ad.thumb_urls is None or len(ad.thumb_urls) == 0:
            return None
        elif index < len(ad.thumb_urls):
            return ad.thumb_urls[index]
        else:
            return ad.thumb_urls[0]

    def get_thumbnail_url_by_name(self,
                                  asset_id: int,
                                  name: str = "sphere") -> Optional[str]:
        """Returns preview url via name extension, if it exists.

        Return value may be None, e.g. in case name not found.
        """
        if asset_id not in self.all_assets:
            return None
        asset_data = self.all_assets[asset_id]
        if asset_data.thumb_urls is None or len(asset_data.thumb_urls) == 0:
            return None

        name = name.lower()
        result_url = None
        for url in asset_data.thumb_urls:
            if name in url.lower():
                result_url = url
                break
        return result_url

    # TODO(Andreas): maybe not URLs...
    def get_large_preview_url_list(self, asset_id: int) -> List[str]:
        """Gets _all_ URLs for an asset's large previews"""

        # TODO(Andreas)
        return []

    def get_large_preview_url(self,
                              asset_id: int,
                              index: int = 0
                              ) -> Optional[str]:
        """Gets URL for an asset's larrge preview"""

        # TODO(Andreas)
        return ""

    def get_watermark_preview_url_list(self,
                                       asset_id: int
                                       ) -> Optional[List[str]]:
        """Gets all URLs for watermarked texture previews"""

        if asset_id not in self.all_assets:
            return []
        asset_data = self.all_assets[asset_id]
        if asset_data.asset_type == assets.AssetType.MODEL:
            return []
        return asset_data.get_type_data().get_watermark_preview_url_list()

    def filter_mesh_texture_maps(
            self,
            asset_id: int,
            asset_textures: List[assets.TextureMap],
            mesh_name: str,
            original_material_name: str = ""
    ) -> Tuple[bool, Optional[str], List[assets.TextureMap]]:
        """Gets all corresponding maps to a given mesh."""

        if asset_id not in self.all_assets:
            return False, None, asset_textures
        asset_data = self.all_assets[asset_id]

        if asset_data.asset_type != assets.AssetType.MODEL:
            return False, None, asset_textures

        return asset_data.get_type_data().filter_mesh_maps(
            asset_textures, mesh_name, original_material_name)

    def get_texture_maps(self,
                         asset_id: int,
                         workflow: str = "METALLIC",
                         size: str = "1K",
                         variant: Optional[str] = None,
                         lod: Optional[str] = None,
                         prefer_16_bit: bool = False
                         ) -> Optional[List[assets.TextureMap]]:
        """Gets all texture maps needed to create a material, brush or HDRI"""

        if asset_id not in self.all_assets:
            return []
        asset_data = self.all_assets[asset_id]
        return asset_data.get_type_data().get_maps(workflow,
                                                   size,
                                                   lod,
                                                   prefer_16_bit)

    def get_mesh(self,
                 asset_id: int,
                 variant: Optional[str] = None,
                 lod: Optional[str] = None,
                 model_type: Optional[assets.ModelType] = None
                 ) -> List[assets.ModelMesh]:
        """Gets the asset mesh that matches with the given LOD"""

        if asset_id not in self.all_assets:
            return []
        asset_data = self.all_assets[asset_id]
        if asset_data.asset_type != assets.AssetType.MODEL:
            return []
        return asset_data.get_type_data().get_mesh(lod, model_type=model_type)

    def get_native_mesh(
            self, asset_id: int, software_ext: str, renderer: str) -> List:
        """Returns native meshes of the given DCC extension and Renderer."""

        if asset_id not in self.all_assets:
            return []
        asset_data = self.all_assets[asset_id]
        if asset_data.asset_type != assets.AssetType.MODEL:
            return None
        return asset_data.get_type_data().get_native_mesh(
            software_ext, renderer)

    def save_cache(self, use_gzip: bool = True) -> None:
        """Saves the cache to self.path_cache"""

        if len(self.path_cache) < 2:
            raise FileNotFoundError("No cache path set!")

        asset_list = [asdict(asset_data)
                      for asset_data in self.all_assets.values()]

        if use_gzip:
            json_str = json.dumps(asset_list, indent=4, default=vars) + "\n"
            json_bytes = json_str.encode("utf-8")
            with gzip.open(self.path_cache, "w") as file_json:
                file_json.write(json_bytes)
        else:
            with open(self.path_cache, "w") as file_json:
                json.dump(asset_list, file_json, indent=4, default=vars)

    def load_cache(self, use_gzip: bool = True) -> None:
        """Loads the cache from self.path_cache"""

        if len(self.path_cache) < 2:
            raise FileNotFoundError("No cache path set!")

        if not os.path.exists(self.path_cache):
            raise FileNotFoundError(f"No saved cache found {self.path_cache}!")

        if use_gzip:
            with gzip.open(self.path_cache, "r") as file_json:
                json_bytes = file_json.read()
            json_str = json_bytes.decode("utf-8")
            asset_list = json.loads(json_str)
        else:
            with open(self.path_cache, "r") as file_json:
                asset_list = json.load(file_json)

        self.all_assets = {}
        for asset_dict in asset_list:
            asset_data = assets.AssetData._from_dict(asset_dict)
            self.all_assets[asset_data.asset_id] = asset_data

        self._verify_cache()

    def _verify_cache(self) -> None:
        """Updates AssetData.is_local in case assets got deleted on disc.
        Updates AssetData.thumbnails in case previews got deleted on disc."""

        pass

    def get_asset_id_list(self,
                          asset_type: Optional[assets.AssetType] = None,
                          purchased: bool = None
                          ) -> List[int]:
        """Return a list of asset IDs in AssetIndex.
        Optionally restricted by per type and/or is_purchased flag.

        Args:
        asset_type: Restrict list to a specific type. Use None for any type.
        purchased: Restrict list to (non-)purchased assets. Use None for both.
        """

        asset_id_list = [
            asset_data.asset_id for asset_data in self.all_assets.values()
            if asset_type is None or asset_data.asset_type == asset_type
        ]
        if purchased is None:
            return asset_id_list

        asset_id_list = [
            asset_id for asset_id in asset_id_list
            if self.all_assets[asset_id].is_purchased == purchased
        ]
        return asset_id_list

    def num_assets(self, asset_type: Optional[assets.AssetType] = None) -> int:
        """Returns the number of assets, optionally per type"""

        asset_id_list = self.get_asset_id_list(asset_type)
        return len(asset_id_list)

    def get_asset_ids_per_type(self) -> Dict:
        """Returns a dictionary with assett IDs per AssetType.
        {AssetType: [asset IDs]}
        """

        asset_ids_per_type = {}
        for asset_type in assets.AssetType:
            asset_ids_per_type[asset_type] = []

        for asset_data in self.all_assets.values():
            asset_ids_per_type[asset_data.asset_type].append(
                asset_data.asset_id)

        return asset_ids_per_type

    def _init_categories(self, categories):
        for category in categories:
            category["asset_count"] = 0
            self._init_categories(category["children"])

    def _count_asset(self, categories, asset_categories):
        num_asset_categories = len(asset_categories)
        for category in categories:
            category_name = category["name"]
            if category_name not in asset_categories:
                continue
            asset_categories.remove(category_name)
            category["asset_count"] += 1
            self._count_asset(category["children"], asset_categories)
            break
        if len(asset_categories) > 0 and len(asset_categories) < num_asset_categories:
            self._count_asset(categories, asset_categories)

    def get_asset_count_per_category(self,
                                     categories: Dict,
                                     purchased: bool = False,
                                     downloaded: bool = False):
        """Fills a "categories dict" with the number of assets
        per category contained in AssetIndex.
        """

        asset_ids_per_type = self.get_asset_ids_per_type()
        self._init_categories(categories)

        # Top level is different,
        # as it actually contains AssetTypes, not categories
        for category in categories:
            asset_type_name = category["name"]
            asset_type = assets.AssetType.type_from_api(asset_type_name)

            # filter depending on purchased and downloaded
            if purchased:
                asset_ids_per_type[asset_type] = [
                    asset_id for asset_id in asset_ids_per_type[asset_type]
                    if self.get_asset(asset_id).is_purchased
                ]
            if downloaded:
                asset_ids_per_type[asset_type] = [
                    asset_id for asset_id in asset_ids_per_type[asset_type]
                    if self.get_asset(asset_id).is_local
                ]

            category["asset_count"] = len(asset_ids_per_type[asset_type])

            for asset_id in asset_ids_per_type[asset_type]:
                asset_data = self.get_asset(asset_id)
                # important copy(), as we remove categories from the list
                asset_categories = asset_data.categories.copy()

                if asset_type_name in asset_categories:
                    asset_categories.remove(asset_type_name)

                self._count_asset(category["children"], asset_categories)
                if len(asset_categories):
                    msg_warn = (f"Did not count all categories ({asset_id})!\n"
                                f"Left over: {asset_categories}\n"
                                f"Asset: {asset_data.categories}\n\n")
                    self.logger.warning(msg_warn)

    def get_files(self, asset_id: int) -> Dict[str, str]:
        """Return a dictionary with all registered files"""

        if asset_id not in self.all_assets:
            return {}

        files_dict = {}  # {filename: attributes string}
        asset_data = self.all_assets[asset_id]
        type_data = asset_data.get_type_data()
        type_data.get_files(files_dict)
        return files_dict

    def flush_is_local(self) -> None:
        """Flushes all is_local flags"""

        for asset_data in self.all_assets.values():
            asset_data.flush_local()

    def flush_is_purchased(self) -> None:
        """Flushes all is_purchased flags"""

        for asset_data in self.all_assets.values():
            asset_data.is_purchased = False

    def flush_queries_by_tab(self, tab: str = "my_assets") -> None:
        """Flushes all queries of a given tab from query cache.

        NOTE: This is NOT protected against concurrent access from other
              threads. Caller has to make sure, there are no outstanding
              get_asset requests, when calling this function.
        """

        for key in list(self.cached_queries.keys()):
            if key[0] == tab:
                del self.cached_queries[key]

    def flush(self, all_assets: bool = False) -> None:
        """Flushes the query cache.

        Args:
        all_assets: If True, not only the query cache,
                    but the entire AssetIndex gets flushed.
        """

        self.cached_queries = {}
        if all_assets:
            self.all_assets = {}

    def _backdoor_validate_parameters(
            self, asset_id: int, asset_name: str, asset_type: str) -> bool:
        if asset_id >= 0:
            raise ValueError("Only negative asset IDs allowed for now")
        if self.get_asset(asset_id) is not None:
            raise ValueError("Asset ID already in use")
        if len(asset_name) == 0:
            raise ValueError("Please specify an asset name")
        if asset_type not in ["HDRIs", "Models", "Textures"]:
            msg = (f"Unknown asset type: {asset_type}\n"
                   "Known types: HDRIs, Models, Textures")
            raise ValueError(msg)
        return True

    def _maps_descs_from_maps(self, tex: assets.Texture) -> None:
        tex.map_descs = {}
        map_descs = tex.map_descs
        for workflow, tex_maps in tex.maps.items():
            if workflow not in map_descs:
                map_descs[workflow] = []
            for _tex_map in tex_maps:
                map_desc = assets.TextureMapDesc(_tex_map.map_type.name,
                                                 "",  # filename_preview
                                                 _tex_map.map_type.name,
                                                 [_tex_map.size],
                                                 [_tex_map.variant])
                map_descs[workflow].append(map_desc)

    def load_asset_from_list(self,
                             asset_id: int,
                             asset_name: str,
                             asset_type: str,
                             size: str,
                             lod: str,
                             workflow_expected: str,
                             file_list_json: str,
                             query_string: str = "my_assets/All Assets",
                             query_chunk_idx: int = -1,
                             query_chunk_size: int = 1000000
                             ) -> bool:
        """Imports an asset from a list of files.

        Arguments:
        asset_id: Has to be negative and unique!
        asset_name: An arbitrary name
        asset_type: One of: "HDRIs", "Models", "Textures"
        size: Only used, if there are multiple sizes found in list of files.
              In that case, a model's default size will be set to this size.
        lod: TODO(Andreas): Currently not in use. Could serve as a fallback
                            similar to size.
        workflow_expected: Used to show a warning on mismatch and as a fallback
                           in case no workflow got identified in list of files.
        file_list_json: List of files in JSON format, see Notion doc on Backdoor
        query_key: A query key as passed to _query_key_to_tuple()
                   All query_ parameters are used to store the imported asset
                   in query cache.
        query_chunk_idx: Query chunk index (e.g. page index)
        query_chunk_size: Query chunk size (e.g. page size)
        """

        if not self._backdoor_validate_parameters(asset_id,
                                                  asset_name,
                                                  asset_type):
            return False

        parameter_info = ("Backdoor import:\n"
                          f"    Asset ID: {asset_id}\n"
                          f"    Name:     {asset_name}\n"
                          f"    Type:     {asset_type}\n"
                          f"    Size:     {size}\n"
                          f"    LOD:      {lod}\n"
                          f"    Workflow: {workflow_expected}\n")
        self.logger.info(parameter_info)

        date_now = datetime.now().strftime("%Y-%m-%d 00:00:00")
        asset_type = assets.AssetType.type_from_api(asset_type)

        asset_data = assets.AssetData(asset_id, asset_type, asset_name)
        asset_data.display_name = asset_name
        asset_data.categories = []
        asset_data.url = None
        asset_data.slug = None
        asset_data.credits = 0
        asset_data.thumb_urls = None
        asset_data.published_at = date_now
        asset_data.is_local = True
        asset_data.downloaded_at = None
        asset_data.is_purchased = True
        asset_data.purchased_at = None
        asset_data.render_custom_schema = None

        if asset_type == assets.AssetType.HDRI:
            asset_data.hdri = assets.Hdri()
        elif asset_type == assets.AssetType.MODEL:
            asset_data.model = assets.Model()
        elif asset_type == assets.AssetType.TEXTURE:
            asset_data.texture = assets.Texture()

        lods = []
        sizes = []
        variants = []
        previews = []
        texture_maps = {}
        meshes = []

        file_list = json.loads(file_list_json)
        for single_file_dict in file_list:
            for key, path_file in single_file_dict.items():
                path, filename = os.path.split(path_file)
                self._analyze_single_file(path,
                                          filename,
                                          workflow_expected,
                                          lods,
                                          sizes,
                                          variants,
                                          previews,
                                          texture_maps,
                                          meshes)
                break  # there's only one entry anyway

        lods = sorted(list(set(lods)))
        sizes = sorted(list(set(sizes)))
        variants = sorted(list(set(variants)))

        # Update actual asset_data with results from file analysis
        if asset_type == assets.AssetType.HDRI:
            _ = self._prepare_hdri_update_asset_data(
                workflow_expected, texture_maps, asset_data)
            self._maps_descs_from_maps(asset_data.hdri.bg)
            self._maps_descs_from_maps(asset_data.hdri.light)
            asset_data.hdri.bg.sizes = sizes
            asset_data.hdri.light.sizes = sizes
        elif asset_type == assets.AssetType.MODEL:
            _ = self._prepare_model_update_asset_data(workflow_expected,
                                                      meshes,
                                                      texture_maps,
                                                      sizes,
                                                      variants,
                                                      lods,
                                                      asset_data)
            self._maps_descs_from_maps(asset_data.model.texture)
            asset_data.model.sizes = sizes
            if len(sizes) > 1:
                asset_data.model.size_default = sizes[0]
            else:
                asset_data.model.size_default = size
            if lods is not None and len(lods) > 0:
                asset_data.model.lods = lods
            else:
                asset_data.model.lods = ["NONE"]
        elif asset_type == assets.AssetType.TEXTURE:
            _ = self._prepare_tex_update_asset_data(
                workflow_expected, texture_maps, sizes, asset_data)
            self._maps_descs_from_maps(asset_data.texture)

        self.all_assets[asset_id] = asset_data

        qt = self._query_key_to_tuple(
            query_string, query_chunk_idx, query_chunk_size)
        if qt in self.cached_queries:
            self.cached_queries[qt].insert(0, asset_id)
        else:
            self.cached_queries[qt] = [asset_id]

        self.logger.info("Backdoor successfully imported asset_name")
        return True
