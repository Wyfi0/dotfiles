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

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Dict, List, Optional, Sequence, Tuple, Union
import os

# API_TYPE_TO_ASSET_TYPE defined at the end of the file (needs AssetType defined)
LODS = [f"LOD{i}" for i in range(5)]
# MAPS_TYPE_NAMES defined at the end of the file (needs MapType defined)
PREVIEWS = ["_atlas",
            "_sphere",
            "_cylinder",
            "_fabric",
            "_preview1",
            "_preview2",
            "_preview3",
            "_preview01",
            "_preview02",
            "_preview03",
            "_flat",
            "_cube",
            "_grid",
            ]
PREVIEW_EXTS_LOWER = [".jpg", ".jpeg", ".png"]
MAP_EXT_LOWER = [".jpg", ".jpeg", ".png", ".tif", ".exr", ".psd"]
SIZES = [f'{i+1}K' for i in range(18)] + ["HIRES", "WM"]
VARIANTS = [f'VAR{i}' for i in range(1, 10)]
WORKFLOWS = ["REGULAR", "METALNESS", "SPECULAR"]
CATEGORY_TRANSLATION = {"Hdrs": "HDRIs"}


class MapType(IntEnum):
    """Supported texture map types.

    NOTE: When extending, existing values MUST NEVER be changed.
    NOTE 2: Derived from IntEnum for easier "to JSON serialization"
    """

    DEFAULT = 1
    UNKNOWN = 1

    ALPHA = 2  # Usually associated with a brush
    ALPHAMASKED = 3
    AO = 4
    BUMP = 5
    BUMP16 = 6
    COL = 7
    DIFF = 8
    DISP = 9
    DISP16 = 10
    EMISSIVE = 11
    ENV = 12  # Environment for an HDRI, typically a .jpg file
    JPG = 12  # Environment for an HDRI, type_code as in ApiResponse
    FUZZ = 13
    GLOSS = 14
    IDMAP = 15
    LIGHT = 16  # Lighting for an HDRI, typically a .exr file
    HDR = 16  # Lighting for an HDRI, type_code as in ApiResponse
    MASK = 17
    METALNESS = 18
    NRM = 19
    NRM16 = 20
    OVERLAY = 21
    REFL = 22
    ROUGHNESS = 23
    SSS = 24
    TRANSLUCENCY = 25
    TRANSMISSION = 26

    @classmethod
    def from_type_code(cls, map_type_code: str):
        if map_type_code in MAPS_TYPE_NAMES:
            return cls[map_type_code]

        map_type_code = map_type_code.split("_")[0]
        if map_type_code in MAPS_TYPE_NAMES:
            return cls[map_type_code]

        return cls.UNKNOWN


class ModelType(IntEnum):
    """Supported formats for Models.

    NOTE: When extending, existing values MUST NEVER be changed.
    NOTE 2: Derived from IntEnum for easier "to JSON serialization"
    """

    FBX = 1
    BLEND = 2
    MAX = 3
    C4D = 4


class AssetType(IntEnum):
    """Supported asset types.

    NOTE: When extending, existing values MUST NEVER be changed.
    NOTE 2: Derived from IntEnum for easier "to JSON serialization"
    """

    UNSUPPORTED = 1
    BRUSH = 2
    HDRI = 3
    MODEL = 4
    TEXTURE = 5
    SUBSTANCE = 6  # still unsupported

    @classmethod
    def type_from_api(cls, api_type_name: str) -> int:
        if api_type_name not in CATEGORY_NAME_TO_ASSET_TYPE:
            return cls.UNSUPPORTED
        return CATEGORY_NAME_TO_ASSET_TYPE[api_type_name]


CATEGORY_NAME_TO_ASSET_TYPE = {"All Assets": None,
                               "Brushes": AssetType.BRUSH,
                               "HDRIs": AssetType.HDRI,
                               "Models": AssetType.MODEL,
                               "Substances": AssetType.SUBSTANCE,
                               "Textures": AssetType.TEXTURE,
                               }


# NOT a data class
# Reason: this way it does not get saved with the asset index.
class AssetStateDownload():
    """Stores asset download related state information."""

    in_progress: bool = False
    cancelled: bool = False
    any_error: bool = False
    directory: Optional[str] = None
    progress: float = 0.001
    downloaded_bytes: int = 1
    recently_downloaded: bool = False
    # Always parse the first error found in the download process
    error: Optional[str] = None

    def start(self) -> None:
        """To be called when a download begins."""

        self.in_progress = True
        self.cancelled = False
        self.any_error = False
        self.error = None
        self.directory = None
        self.progress = 0.001
        self.downloaded_bytes = 1

    def end(self) -> None:
        """To be called after a download has ended (regardless of result)."""

        self.in_progress = False
        self.cancelled = False
        self.progress = 0.001
        # Do not touch self.any_error!

    def is_in_progress(self) -> bool:
        """Returns True, if a download is currently in progress."""

        return self.in_progress

    def cancel(self, reset: bool = False) -> None:
        """Cancels a download."""

        self.cancelled = not reset

    def is_cancelled(self) -> bool:
        """Returns True, if the download is supposoed to be cancelled."""

        return self.cancelled

    def set_error(
            self, error_msg: Optional[str] = None, reset: bool = False) -> None:
        """To be called if any kind of error occured during download."""

        self.any_error = not reset
        if self.error is None and error_msg is not None:
            self.error = error_msg

    def has_error(self) -> bool:
        """Returns True, if any error occurred during download."""

        return self.any_error

    def set_directory(self, directory: str) -> None:
        """Sets current download directory."""

        self.directory = directory

    def get_directory(self) -> Optional[str]:
        """Returns current download directory."""

        return self.directory

    def set_progress(self, progress: float) -> None:
        """Sets current download progress in percent [0.0-1.0]."""

        self.progress = min(max(0.001, progress), 1.0)

    def get_progress(self) -> float:
        """Returns current download progress, guaranteed non-zero."""

        return self.progress

    def set_downloaded_bytes(self, downloaded_bytes: int) -> None:
        """Sets currently downloaded bytes."""

        self.downloaded_bytes = max(1, downloaded_bytes)

    def get_downloaded_bytes(self) -> int:
        """Returns currently downloaded bytes, guaranteed non-zero."""

        return self.downloaded_bytes

    def set_recently_downloaded(self, recently_downloaded: bool) -> None:
        """Marks the asset as recently, successfully downloaded."""

        self.recently_downloaded = recently_downloaded

    def get_recently_downloaded(self) -> bool:
        """Returns True, if the asset got successfully downloaded, recently."""

        return self.recently_downloaded


# NOT a data class
# Reason: this way it does not get saved with the asset index.
class AssetStatePurchase():
    """Stores asset purchase related state information."""

    in_progress: bool = False

    def start(self) -> None:
        """To be called when a purchase begins."""

        self.in_progress = True

    def end(self) -> None:
        """To be called after a purchase has ended (regardless of result)."""

        self.in_progress = False

    def is_in_progress(self) -> bool:
        """Returns True, if a purchase is currently in progress."""

        return self.in_progress


class AssetState():
    """The volatile status of an asset during addon operation.
    E.g. download status in .dl

    Note: Upon saving and reloading AssetIndex from disk information contained
          in here will reset. State does _not_ get stored to disk.
    """

    # TODO(Andreas): Vanish into nothingness upon saving the asset index (works
    #                already, kind of).
    #                Create new instance upon load.

    def __init__(self):
        self.purchase = AssetStatePurchase()
        self.dl = AssetStateDownload()


class AssetDataRuntime():
    """Stores additional asset data, which will only be valid during runtime.
    Content of this class will not be loaded from disk.
    """

    size_current: Optional[str] = None

    def store_current_size(self, size: Optional[str]) -> None:
        """Can be used to store a size, e.g. to change the size used for the
        import button.
        """

        self.size_current = size

    def get_current_size(self) -> Optional[str]:
        """Returns a previously stored size."""

        return self.size_current


# TODO(Andreas): Add a workflow enum
# TODO(Andreas): Add a LOD enum

def _cond_set(new, old):
    return new if new is not None else old


def find_closest_size(size: str,
                      size_list: List[str]
                      ) -> Optional[str]:
    """Tries to find an alternative size.
    The distance inside the SIZES list is used as metric of proximity.
    """

    if len(size_list) == 0:
        return None

    idx_size_wanted = SIZES.index(size)
    dist_min = len(SIZES)
    size_best_fit = None
    for idx_size, size_test in enumerate(SIZES):
        dist = abs(idx_size_wanted - idx_size)
        if size_test in size_list and dist < dist_min:
            dist_min = dist
            size_best_fit = size_test
    return size_best_fit


class BaseAsset:
    """Function prototypes common to all asset types."""

    def update(self, type_data_new, purge_maps: bool = False) -> None:
        """Updates asset's data

        Args:
            type_data_new: Type Any of Brush, Hdri, Model or Texture
            purge_maps: If True any existing map entries will be thrown away
        """

        raise NotImplementedError


class BaseTex(BaseAsset):
    """Function prototypes common to all image based asset types."""

    def get_workflow_list(self) -> List[str]:
        """Returns a list of all available workflows"""

        raise NotImplementedError

    def get_workflow(self, workflow: str) -> str:
        """Verifies workflow is available or returns fallback"""

        raise NotImplementedError

    def get_size_list(self,
                      incl_watermarked: bool = False,
                      local_only: bool = False
                      ) -> List[str]:
        """Returns a list of all available sizes"""

        raise NotImplementedError

    def get_size(self, size: str, local_only: bool = False) -> str:
        """Verifies size is available, otherwise returns closest one.

        Raises: KeyError, if no size was found at all.
        """

        raise NotImplementedError

    def get_variant_list(self) -> List[str]:
        """Returns a list of all available variants"""

        raise NotImplementedError

    def get_watermark_preview_url_list(self) -> Optional[List[str]]:
        """Returns a list of URLs needed for watermarked material assignment"""

        raise NotImplementedError

    def get_map_type_list(self, workflow: str) -> List[MapType]:
        """Returns a list of MapType needed for a workflow.

        Raises: KeyError, if workflow not found.
        """
        raise NotImplementedError

    def get_maps(self,
                 workflow: str = "REGULAR",
                 size: str = "1K",
                 lod: Optional[str] = None,
                 prefer_16_bit: bool = False
                 ) -> List:
        """Returns a list of Texture needed for workflow and size

        Return value: Type List[TextureMap]

        Raises: KeyError, if workflow not found.
        """

        raise NotImplementedError


@dataclass
class TextureMap:
    """Container object for a texture map.
    This class represents actual files on disc.
    Instances of this class exist only,
    if the respective files have been downloaded and found.
    """

    directory: str = ""
    filename: str = ""
    lod: Optional[str] = None
    map_type: MapType = MapType.UNKNOWN
    size: str = "1K"  # Short string, e.g., "1K"
    variant: Optional[str] = None

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "map_type" not in d:
            raise KeyError("map_type")
        new = cls(**d)
        new.map_type = MapType(new.map_type)
        return new

    def __eq__(self, other) -> bool:
        """Equality operator

        Args:
            other: Type TextureMap

        NOTE: The result does not imply identity!!!
              Instead two TextureMaps are considered equal,
              if they are used in the same "slot".
              map_type, size and variant need to match,
              but filename does NOT need to match.
              Reason is the use of this comparison during
              updating AssetData.
        """

        return self._key_tuple() == other._key_tuple()

    def _key_tuple(self) -> Tuple:
        """Merges relevant members into a Tuple"""

        return (self.map_type, self.size, self.variant, self.lod)

    def get_path(self):
        return os.path.join(self.directory, self.filename)


@dataclass
class TextureMapDesc:
    """Container object for a texture map description.
    Instances of this class get created after an asset has been queried.
    """

    display_name: str  # Beauty name for UI display
    filename_preview: str
    map_type_code: str  # "type_code" field as retrieved from the API
    sizes: List[str]  # List of sizes, e.g., ["1K", "2K"]
    variants: List[str]  # List of variants, e.g. ["VAR1", "VAR2"]

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "map_type_code" not in d:
            raise KeyError("map_type_code")
        new = cls(**d)
        return new

    def __eq__(self, other) -> bool:
        # order of comparisons: "importance" of keys
        if self.map_type_code != other.map_type_code:
            return False
        if self.sizes != other.sizes:
            return False
        if self.variants != other.variants:
            return False
        if self.display_name != other.display_name:
            return False
        if self.filename_preview != other.filename_preview:
            return False
        return True

    def get_map_type(self):
        return MapType.from_type_code(self.map_type_code)


@dataclass
class Texture(BaseTex):
    """Container object for a Texture."""

    # Texture options for display in UI
    # sizes, variants and lods are sets of all contained in an asset.
    # It is NOT guaranteed, all of them exist in all channels/workflows.
    lods: Optional[Sequence[str]] = None  # List of lods, e.g. ["LOD1", "LOD3"]
    map_descs: Optional[Dict[str, List[TextureMapDesc]]] = None  # {workfl. : [TextureMapsDesc]}
    maps: Optional[Dict[str, Sequence[TextureMap]]] = field(default_factory=dict)  # {workfl. : [TextureMaps]}
    sizes: Optional[Sequence[str]] = None  # List of sizes, e.g., ["1K", "2K"]
    variants: Optional[Sequence[str]] = None  # List of variants, e.g. ["VAR1", "VAR2"]
    watermarked_urls: Optional[Sequence[str]] = None
    # Width (x) and Height (y) of the real world dimension of the texture
    real_world_dimension: Optional[Tuple[int, int]] = None

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "map_descs" not in d:
            raise KeyError("map_descs")
        if "maps" not in d:
            raise KeyError("maps")

        # Replace sub-dicts describing our class instances
        # with actual class instances
        tex_maps_desc_dict = d["map_descs"]
        if tex_maps_desc_dict is not None:
            for workflow, tex_map_desc_list in tex_maps_desc_dict.items():
                for idx_map_desc, tex_map_desc in enumerate(tex_map_desc_list):
                    tex_map_desc_list[idx_map_desc] = TextureMapDesc._from_dict(tex_map_desc)

        tex_maps_dict = d["maps"]
        if tex_maps_dict is not None:
            for workflow, tex_map_list in tex_maps_dict.items():
                for idx_map, tex_map in enumerate(tex_map_list):
                    tex_map_list[idx_map] = TextureMap._from_dict(tex_map)

        new = cls(**d)
        return new

    def _map_key_dict(self, workflow: str) -> Dict:
        """Returns a dictionary with all texture maps of a given workflow,
        index by key_tuples (see TextureMap)."""
        return {tex_map._key_tuple():
                tex_map for tex_map in self.maps[workflow]
                }

    def _update_new_sizes(self, type_data_new) -> None:
        if type_data_new.sizes is None or len(type_data_new.sizes) == 0:
            return
        if self.sizes is None:
            self.sizes = []
        for _size in type_data_new.sizes:
            if _size not in self.sizes:
                self.sizes.append(_size)
        # TODO(Andreas): Need sorting?
        #                Actually this should happen only with size WM,
        #                which is fine at the end

    def update(self, type_data_new, purge_maps: bool = False) -> None:
        """Updates Texture data

        Args:
            type_data_new: Type Texture, the instance to update from
            purge_maps: If True any existing map entries will be thrown away
        """

        if type_data_new is None:
            return

        self.map_descs = _cond_set(type_data_new.map_descs, self.map_descs)
        self._update_new_sizes(type_data_new)
        self.variants = _cond_set(type_data_new.variants, self.variants)
        self.lods = _cond_set(type_data_new.lods, self.lods)
        self.watermarked_urls = _cond_set(type_data_new.watermarked_urls,
                                          self.watermarked_urls)

        if purge_maps:
            self.maps = {}

        for workflow, tex_maps_new in type_data_new.maps.items():
            if workflow not in self.maps:
                self.maps[workflow] = tex_maps_new
                continue

            tex_map_dict = self._map_key_dict(workflow)
            for tex_map_new in tex_maps_new:
                key = tex_map_new._key_tuple()
                if key in tex_map_dict:
                    tex_map_dict[key].filename = tex_map_new.filename
                else:
                    self.maps[workflow].append(tex_map_new)

    def is_local(self,
                 workflow: str = "REGULAR",
                 size: str = "1K",
                 prefer_16_bit: bool = False,
                 do_filecheck: bool = False) -> bool:
        """Checks if the texture files are local"""

        if workflow not in self.maps:
            return False

        map_types = self.get_map_types(workflow, prefer_16_bit)
        tex_maps = self.get_maps(workflow, size, prefer_16_bit)
        for tex_map in tex_maps:
            try:
                map_types.remove(tex_map.map_type)
            except ValueError:
                # deliberately surpressed
                # e.g. variants lead to type occurring multiple times
                pass
        # TODO(Andreas): if do_filecheck
        return len(map_types) == 0

    def get_workflow_list(self) -> List[str]:
        """Returns list of all available workflows"""

        return list(self.map_descs.keys())

    def get_workflow(self, workflow: str = "REGULAR") -> Optional[str]:
        """Verifies workflow is available or returns fallback"""

        if workflow in self.map_descs:
            return workflow
        elif "METALNESS" in self.map_descs:
            return "METALNESS"
        elif "REGULAR" in self.map_descs:
            return "REGULAR"
        elif len(self.map_descs) >= 1:
            return list(self.map_descs.keys())[0]
        else:
            return None

    def sort_size_key(self, size):
        try:
            return int(size[:-1])
        except ValueError:
            # Any value off the format "2K", "4K"
            # Highest texture size is 18, 20 to force end of the list
            if "WM" in size:
                return 20
            elif "HIRES" in size:
                return 21
            else:
                return 22

    def get_size_list(self,
                      incl_watermarked: bool = False,
                      local_only: bool = False
                      ) -> List[str]:
        """Returns list of all available sizes"""

        if local_only:
            sizes = set()
            for workflow in self.maps:
                for tex_map in self.maps[workflow]:
                    sizes.add(tex_map.size)
        else:
            sizes = self.sizes.copy()

        sizes = sorted(list(sizes), key=self.sort_size_key)

        if not incl_watermarked and "WM" in sizes:
            sizes.remove("WM")

        return sizes

    def get_size(self, size: str = "1K", local_only: bool = False) -> str:
        """Verifies size is available, otherwise returns closest one.

        Raises: KeyError, if no size was found at all.
        """

        size_list = self.get_size_list(local_only=local_only)
        if size == "WM":
            return size
        elif size in size_list:
            return size

        size_best_fit = find_closest_size(size, size_list)
        if size_best_fit is None:
            raise KeyError(f"No suitable size found (request: {size})")
        return size_best_fit

    def get_variant_list(self) -> List[str]:
        """Returns list of all available variants"""

        return self.variants

    def get_lod_list(self) -> List[str]:
        """Returns list of all available LODs"""

        return self.lods

    def get_watermark_preview_url_list(self) -> Optional[List[str]]:
        """Returns list of URLs needed for watermarked material assignment"""

        return self.watermarked_urls

    def get_map_type_list(self, workflow: str = "REGULAR") -> List[MapType]:
        """Returns list of MapType needed for a workflow.

        Raises: KeyError, if workflow not found.
        """

        if workflow not in self.map_descs:
            raise KeyError(f"Workflow not found: {workflow}")

        map_descs = self.map_descs[workflow]
        return [
            map_desc.get_map_type()
            for map_desc in map_descs
        ]

    def get_map_type_code_list(self, workflow: str = "REGULAR") -> List[str]:
        """Returns list of type_code needed for a workflow.

        Raises: KeyError, if workflow not found.
        """

        if workflow not in self.map_descs:
            raise KeyError(f"Workflow not found: {workflow}")

        map_descs = self.map_descs[workflow]
        return [map_desc.map_type_code for map_desc in map_descs]

    def get_maps(self,
                 workflow: str = "REGULAR",
                 size: str = "1K",
                 lod: Optional[str] = None,
                 prefer_16_bit: bool = False,
                 suffix_list: List[str] = [".png", ".tif", ".jpg"]
                 ) -> List[TextureMap]:
        """Returns list of Texture needed for workflow and size."""

        if workflow not in self.maps:
            return []

        get_lod = lod is not None

        # TODO(Andreas): Use result of call below to check for any missing maps
        #                and then check for file alternatives.
        #                This change got put on hold after discussion with Patrick.
        # self.get_asset_map_type_list(asset_id,  # aaargh, we do not have this here :(
        #                              workflow=workflow,
        #                              prefer_16_bit=prefer_16_bit)

        tex_map_dict = {}  # {MapType : [TextureMap]}
        for tex_map in self.maps[workflow]:
            # TODO(Andreas): deliver fallback size maps in case map is not found
            if tex_map.size != size:
                continue
            # TODO(Andreas): deliver alternative lod, if not found?
            tex_has_lod = tex_map.lod is not None and tex_map.lod != "NONE"
            if get_lod and tex_has_lod and tex_map.lod != lod:
                continue

            tex_map_dict[tex_map.map_type] = tex_map_dict.get(tex_map.map_type,
                                                              []
                                                              ) + [tex_map]

        # Decide between 8-Bit and 16-Bit, if both are available
        if MapType.BUMP in tex_map_dict and MapType.BUMP16 in tex_map_dict:
            if prefer_16_bit:
                del tex_map_dict[MapType.BUMP]
            else:
                del tex_map_dict[MapType.BUMP16]
        if MapType.DISP in tex_map_dict and MapType.DISP16 in tex_map_dict:
            if prefer_16_bit:
                del tex_map_dict[MapType.DISP]
            else:
                del tex_map_dict[MapType.DISP16]
        if MapType.NRM in tex_map_dict and MapType.NRM16 in tex_map_dict:
            if prefer_16_bit:
                del tex_map_dict[MapType.NRM]
            else:
                del tex_map_dict[MapType.NRM16]

        tex_maps = []

        # Get rid of multiple files for the same texture (e.g. .png and .psd)
        for map_type, tex_map_list in tex_map_dict.items():
            if len(tex_map_list) == 1:
                tex_maps.append(tex_map_list[0])
                continue
            found = False
            for suffix_preferred in suffix_list:
                for tex_map in tex_map_list:
                    _, suffix = os.path.splitext(tex_map.filename)
                    if suffix == suffix_preferred:
                        tex_maps.append(tex_map)
                        found = True
            if not found:
                tex_maps.append(tex_map_list[0])
                print(f"Multiple texture files per MapType ({map_type.name}), but none with preferred suffix!")

        return tex_maps

    def get_preview_filename_list(self,
                                  workflow: str = "REGULAR"
                                  ) -> List[str]:
        """Returns list of preview filenames of all channels.

        Raises: KeyError, if workflow not found.
        """

        workflow = self.get_workflow(workflow)
        if workflow is None:
            raise KeyError(f"Workflow not found: {workflow}")

        tex_maps = self.maps[workflow]
        preview_filenames = [tex_map.filename_preview for tex_map in tex_maps]
        return preview_filenames

    def get_files(self, files_dict: Dict) -> None:
        """Adds all registered texture files to dict_files.
        {filename: attribute string}"""

        for workflow, tex_map_list in self.maps.items():
            for tex_map in tex_map_list:
                path = tex_map.get_path()
                tex_attr = f"{workflow}, {tex_map.map_type.name}, {tex_map.size}"
                if tex_map.lod is not None:
                    tex_attr += f", {tex_map.lod}"
                if tex_map.variant is not None:
                    tex_attr += f", {tex_map.variant}"
                files_dict[path] = tex_attr

    def flush_local(self):
        """Resets all information about local files."""

        self.maps = {}


@dataclass
class Hdri(BaseTex):
    """Container object for an HDRI."""

    bg: Texture = None  # Background texture with single map of type JPG
    light: Texture = None  # Light texture with single map of type HDR

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "bg" not in d:
            raise KeyError("bg")
        if "light" not in d:
            raise KeyError("light")

        bg = Texture._from_dict(d["bg"])
        light = Texture._from_dict(d["light"])
        return cls(bg, light)

    def update(self, type_data_new, purge_maps: bool = False) -> None:
        """Updates Hdri data

        Args:
            type_data_new: Type Hdri, the instance to update from
            purge_maps: If True any existing map entries will be thrown away
        """

        if type_data_new is None:
            return
        self.bg.update(type_data_new.bg, purge_maps)
        self.light.update(type_data_new.light, purge_maps)

    def get_workflow_list(self) -> List[str]:
        """Returns list of all available workflows"""

        # Currently assuming workflows are identical for light + bg
        return self.bg.get_workflow_list()

    def get_workflow(self, workflow: str = "REGULAR") -> Optional[str]:
        """Verifies workflow is available or returns fallback"""

        # Currently assuming workflows are identical for light + bg
        return self.bg.get_workflow(workflow)

    def get_size_list(self,
                      incl_watermarked: bool = False,
                      local_only: bool = False
                      ) -> List[str]:
        """Returns list of all available sizes"""

        # Currently assuming sizes are identical for light + bg
        return self.bg.get_size_list(incl_watermarked, local_only)

    def get_size(self, size: str, local_only: bool = False) -> str:
        """Verifies size is available, otherwise returns closest one.

        Raises: KeyError, if no size was found at all.
        """

        # Currently assuming sizes are identical for light + bg
        return self.bg.get_size(size, local_only)

    def get_variant_list(self) -> List[str]:
        """Returns list of all available variants"""

        # Currently assuming variants are identical for light + bg
        return self.bg.get_variant_list()

    def get_watermark_preview_url_list(self) -> Optional[List[str]]:
        """Returns list of URLs needed for watermarked material assignment"""

        return self.bg.get_watermark_preview_url_list()

    def get_map_type_list(self, workflow: str = "REGULAR") -> List[MapType]:
        """Returns list of MapType needed for a workflow.

        Raises: KeyError, if workflow not found.
        """

        map_types = self.bg.get_map_type_list()
        map_types.extend(self.light.get_map_type_list())
        return map_types

    def get_map_type_code_list(self, workflow: str = "REGULAR") -> List[str]:
        """Returns list of type_code needed for a workflow.

        Raises: KeyError, if workflow not found.
        """

        map_codes = self.bg.get_map_type_code_list()
        map_codes.extend(self.light.get_map_type_code_list())
        return map_codes

    def get_maps(self,
                 workflow: str = "REGULAR",
                 size: str = "1K",
                 lod: Optional[str] = None,
                 prefer_16_bit: bool = False
                 ) -> List[TextureMap]:
        """Returns list of Texture needed for workflow and size.

        Raises: KeyError, if workflow not found.
        """

        tex_maps = self.bg.get_maps(workflow, size, lod, prefer_16_bit)
        tex_maps.extend(self.light.get_maps(
            workflow, size, lod, prefer_16_bit))
        return tex_maps

    def get_files(self, files_dict: Dict) -> None:
        """Adds all registered texture files to dict_files.
        {filename: attribute string}"""

        self.bg.get_files(files_dict)
        self.light.get_files(files_dict)

    def flush_local(self):
        """Resets all information about local files."""

        try:
            self.bg.flush_local()
        except AttributeError:
            pass  # no backgound texture
        try:
            self.light.flush_local()
        except AttributeError:
            pass  # no light texture


@dataclass
class Brush(BaseTex):
    """Container object for a Brush."""

    alpha: Texture  # Texture with single map of type ALPHA

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "alpha" not in d:
            raise KeyError("alpha")

        alpha = Texture._from_dict(d["alpha"])
        return cls(alpha)

    def update(self, type_data_new, purge_maps: bool = False) -> None:
        """Updates Brush data

        Args:
            type_data_new: Type Brush, the instance to update from
            purge_maps: If True any existing map entries will be thrown away
        """

        if type_data_new is None:
            return
        self.alpha.update(type_data_new.alpha, purge_maps)

    def get_workflow_list(self) -> List[str]:
        """Returns list of all available workflows"""

        return list(self.alpha.map_descs.keys())

    def get_workflow(self, workflow: str = "REGULAR") -> Optional[str]:
        """Verifies workflow is available or returns fallback"""

        return self.alpha.get_workflow(workflow)

    def get_size_list(self,
                      incl_watermarked: bool = False,
                      local_only: bool = False
                      ) -> List[str]:
        """Returns list of all available sizes"""

        return self.alpha.get_size_list(incl_watermarked, local_only)

    def get_size(self, size: str, local_only: bool = False) -> str:
        """Verifies size is available, otherwise returns closest one.

        Raises: KeyError, if no size was found at all.
        """

        return self.alpha.get_size(size, local_only)

    def get_variant_list(self) -> List[str]:
        """Returns list of all available variants"""

        return self.alpha.get_variant_list()

    def get_watermark_preview_url_list(self) -> Optional[List[str]]:
        """Returns list of URLs needed for watermarked material assignment"""

        return self.alpha.get_watermark_preview_url_list()

    def get_map_type_list(self, workflow="REGULAR") -> List[MapType]:
        """Returns list of MapType needed for a workflow.

        Raises: KeyError, if workflow not found.
        """

        return self.alpha.get_map_type_list()

    def get_maps(self,
                 workflow: str = "REGULAR",
                 size: str = "1K",
                 lod: Optional[str] = None,
                 prefer_16_bit: bool = False
                 ) -> List[TextureMap]:
        """Returns list of Texture needed for workflow and size.

        Raises: KeyError, if workflow not found.
        """

        return self.alpha.get_maps(workflow, size, lod, prefer_16_bit)

    def get_files(self, files_dict: Dict) -> None:
        """Adds all registered texture files to dict_files.
        {filename: attribute string}"""

        self.alpha.get_files(files_dict)

    def flush_local(self):
        """Resets all information about local files."""

        try:
            self.alpha.flush_local()
        except AttributeError:
            pass  # no alpha texture


@dataclass
class ModelMesh:
    """Container object for a Model file.
    This class represents actual files on disc.
    Instances of this class exist only,
    if the respective files have been downloaded and found.
    """

    directory: str
    filename: str
    lod: str
    model_type: ModelType

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "model_type" not in d:
            raise KeyError("model_type")
        new = cls(**d)
        new.model_type = ModelType(new.model_type)
        return new

    def get_path(self):
        return os.path.join(self.directory, self.filename)


@dataclass
class Model(BaseAsset):
    """Container object for a Model."""

    # lods: List of lods, e.g., ["LOD0", "LOD2"]
    # Will be None, if "has_lods" is false,
    # otherwise empty list until populated.
    lods: Optional[Sequence[str]] = None
    meshes: Optional[Sequence[ModelMesh]] = None
    sizes: Optional[Sequence[str]] = None  # List of sizes, e.g., ["1K", "2K"]
    size_default: Optional[str] = None  # reported in included_resolution
    texture: Optional[Texture] = None
    variants: Optional[Sequence[str]] = None  # List of variants, e.g. ["VAR1", "VAR2"]

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "meshes" not in d:
            raise KeyError("meshes")
        if "texture" not in d:
            raise KeyError("texture")

        # Replace sub-dicts describing our class instances
        # with actual class instances
        mesh_list = d["meshes"]
        if mesh_list is not None:
            for idx_model, mesh_dict in enumerate(mesh_list):
                mesh_list[idx_model] = ModelMesh._from_dict(mesh_dict)

        tex_dict = d["texture"]
        if tex_dict is not None:
            d["texture"] = Texture._from_dict(tex_dict)

        new = cls(**d)
        return new

    def update(self, type_data_new, purge_maps: bool = False) -> None:
        """Updates Model data

        Args:
            type_data_new: Type Model, the instance to update from
            purge_maps: If True any existing map entries will be thrown away
        """

        if type_data_new is None:
            return
        self.meshes = _cond_set(type_data_new.meshes, self.meshes)
        self.lods = _cond_set(type_data_new.lods, self.lods)
        if self.lods is None or len(self.lods) == 0:
            self.lods = ["NONE"]
        self.sizes = _cond_set(type_data_new.sizes, self.sizes)
        self.variants = _cond_set(type_data_new.variants, self.variants)
        if self.texture is None:
            self.texture = type_data_new.texture
        else:
            self.texture.update(type_data_new.texture, purge_maps)

    def get_workflow_list(self) -> List[str]:
        """Returns list of all available workflows"""

        if self.texture is None:
            return []
        # Model has no TextureMapDescs in Texture
        return list(self.texture.maps.keys())

    def get_workflow(self, workflow: str = "REGULAR") -> Optional[str]:
        """Verifies workflow is available or returns fallback"""

        if self.texture is None:
            return None
        return self.texture.get_workflow(workflow)

    def get_size_list(self,
                      incl_watermarked: bool = False,
                      local_only: bool = False
                      ) -> List[str]:
        """Returns list of all available sizes"""

        incl_watermarked = False  # No watermarked textures for Models
        if local_only:
            if self.texture is None:
                return []
            return self.texture.get_size_list(incl_watermarked, local_only)
        else:
            return self.sizes

    def get_size(self, size: str, local_only: bool = False) -> str:
        """Verifies size is available, otherwise returns closest one.

        Raises: KeyError, if no size was found at all.
        """

        if self.texture is not None:
            return self.texture.get_size(size, local_only)

        # Not local
        size = find_closest_size(size, self.sizes)
        return size

    def get_variant_list(self) -> List[str]:
        """Returns list of all available variants"""
        if self.texture is None:
            return []
        return self.texture.get_variant_list()

    def get_watermark_preview_url_list(self) -> Optional[List[str]]:
        """Returns list of URLs needed for watermarked material assignment"""

        if self.texture is None:
            return []
        return self.texture.get_watermark_preview_url_list()

    def get_map_type_list(self, workflow: str = "") -> List[MapType]:
        """Returns list of MapType needed for a workflow.

        Raises: KeyError, if workflow not found.
        """

        if self.texture is None:
            return []
        return self.texture.get_map_type_list(workflow)

    def filter_mesh_maps(
            self,
            asset_maps: List[TextureMap],
            mesh_name: str,
            original_material_name: str = ""
    ) -> Tuple[bool, str, List[TextureMap]]:
        """Filters the maps in asset map list to get the corresponding map
        set for a given mesh (FBX Importing).

        NOTE: Currently filtering by placeholder material name and,
        if not material found, by the mesh name.

        TODO(Joao): add filter for variants"""

        mesh_maps = []

        name_mat_imported_lower = original_material_name.lower()
        ends_remastered = name_mat_imported_lower.endswith("_mat")
        contains_remastered = "_mat_" in name_mat_imported_lower

        # if "_mat" in the original fbx material, consider remastered asset
        if ends_remastered or contains_remastered:
            pos_remastered = name_mat_imported_lower.rfind("_mat", 1)
            base_map_name = original_material_name[:pos_remastered]
        else:
            base_map_name = mesh_name.split("_")[0]

        for _tex in asset_maps:
            if base_map_name.lower() in _tex.filename.lower():
                mesh_maps.append(_tex)

        if len(mesh_maps) == 0:
            return False, base_map_name, asset_maps

        return True, base_map_name, mesh_maps

    def get_map_type_code_list(self, workflow: str = "REGULAR") -> List[str]:
        """Returns list of type_code needed for a workflow.

        Raises: KeyError, if workflow not found.
        """

        map_codes = self.texture.get_map_type_code_list(workflow)
        return map_codes

    def get_maps(self,
                 workflow: str = "REGULAR",
                 size: str = "1K",
                 lod: Optional[str] = None,
                 prefer_16_bit: bool = False
                 ) -> List[TextureMap]:
        """Returns list of Texture needed for workflow and size.

        Raises: KeyError, if workflow not found.
        """

        if self.texture is None:
            return []
        return self.texture.get_maps(workflow, size, lod, prefer_16_bit)

    def get_lod_list(self) -> List[str]:
        """Returns list of all available LODs"""

        return self.lods

    def find_closest_lod(self, lod: str) -> Optional[str]:
        """Tries to find an alternative LOD.
        The distance inside the LODS list is used as metric of proximity.
        """

        if lod == "NONE":
            lod = "LOD1"
        idx_lod_wanted = LODS.index(lod)
        dist_min = len(LODS)
        lod_best_fit = None
        for idx_lod, lod_test in enumerate(LODS):
            dist = abs(idx_lod_wanted - idx_lod)
            if lod_test in self.lods and dist < dist_min:
                dist_min = dist
                lod_best_fit = lod_test
        if lod_best_fit is None:
            return self.lods[0]
        return lod_best_fit

    def get_lod(self, lod: str) -> Optional[str]:
        """Verifies LOD is available, otherwise returns the next available."""

        if lod in self.lods:
            return lod
        lod_best_fit = self.find_closest_lod(lod)
        return lod_best_fit

    def get_native_mesh(self, software_ext: str, renderer: str) -> List:
        """Returns native meshes of the given DCC extension and Renderer."""

        meshes = []
        for mesh in self.meshes:
            filename, file_ext = os.path.splitext(mesh.filename)
            match_renderer = renderer.lower() in filename.lower()
            match_ext = file_ext.lower() == software_ext.lower()

            if not match_ext or not match_renderer:
                continue

            meshes.append(mesh)

        return meshes

    def has_mesh(self,
                 model_type: Optional[ModelType],
                 native_only: bool = False
                 ) -> bool:
        """Returns True, if a mesh file exists."""

        if self.meshes is None:
            return False

        for mesh in self.meshes:
            has_desired = mesh.model_type == model_type
            has_fbx = not native_only and mesh.model_type == ModelType.FBX
            if has_desired or has_fbx:
                return True
        return False

    def get_mesh(self,
                 lod: str = "LOD1",
                 model_type: Optional[ModelType] = None
                 ) -> List[ModelMesh]:
        """Returns meshes with the given LOD."""

        if self.meshes is None:
            return []

        if lod is None:
            lod = "LOD1"
        meshes = []
        for mesh in self.meshes:
            if model_type is not None and mesh.model_type != model_type:
                continue
            if mesh.lod != lod:
                continue
            meshes.append(mesh)
        return meshes

    def get_files(self, files_dict: Dict) -> None:
        """Adds all registered files (textures and meshes) to dict_files.
        {filename: attribute string}"""

        for mesh in self.meshes:
            path = mesh.get_path()
            mesh_attr = f"{mesh.model_type.name}"
            if mesh.lod is not None:
                mesh_attr += f", {mesh.lod}"
            files_dict[path] = mesh_attr

        self.texture.get_files(files_dict)

    def flush_local(self):
        """Resets all information about local files."""

        self.meshes = None
        try:
            self.texture.flush_local()
        except AttributeError:
            pass  # no texture


@dataclass
class AssetData:
    """Container object for an asset."""

    asset_id: int
    asset_type: AssetType
    # asset_name: e.g. for filenames, key "asset_name" in ApiResponse
    asset_name: str
    # display_name: Beauty name for UI display, key "name" in ApiResponse
    display_name: Optional[str] = None
    categories: Optional[Sequence[str]] = None
    url: Optional[str] = None
    slug: Optional[str] = None
    credits: Optional[int] = None  # key "credit" in ApiResponse
    # preview: Optional[str] = None
    thumb_urls: Optional[Sequence[str]] = None
    published_at: Optional[str] = None
    # is_local: None until proven true or false.
    # Indicates locality only for at least one "flavour".
    is_local: Optional[bool] = None
    # UTC, seconds since epoch
    downloaded_at: Optional[int] = None
    # is_purchased: None until proven true or false.
    is_purchased: Optional[bool] = None
    # UTC, seconds since epoch
    purchased_at: Optional[int] = None
    # render_custom_schema: Filled with what ever meta data
    # ApiResponse contains for this key.
    render_custom_schema: Optional[Dict] = None

    # Treat below as a "one of",
    # where only set if the given asset type is assigned.
    # Best retrieved via get_type_data().
    brush: Optional[Brush] = None
    hdri: Optional[Hdri] = None
    model: Optional[Model] = None
    texture: Optional[Texture] = None

    # Not preserved, when loaded from disk
    state: AssetState = field(default_factory=lambda: AssetState())
    runtime: AssetDataRuntime = field(default_factory=lambda: AssetDataRuntime())

    @classmethod
    def _from_dict(cls, d: Dict):
        """Alternate constructor,
        used after loading AssetIndex from JSON to reconstruct class.
        """

        if "asset_type" not in d:
            raise KeyError("asset_type")

        new = cls(**d)
        new.asset_type = AssetType(new.asset_type)
        if new.brush is not None:
            new.brush = Brush._from_dict(new.brush)
        elif new.hdri is not None:
            new.hdri = Hdri._from_dict(new.hdri)
        elif new.model is not None:
            new.model = Model._from_dict(new.model)
        elif new.texture is not None:
            new.texture = Texture._from_dict(new.texture)

        new.state = AssetState()
        new.runtime = AssetDataRuntime()

        return new

    def get_type_data(self) -> Union[Texture, Hdri, Brush, Model]:
        """Returns either brush, hdri, model or
        texture based on asset's type.
        """

        if self.asset_type == AssetType.BRUSH:
            return self.brush
        elif self.asset_type == AssetType.HDRI:
            return self.hdri
        elif self.asset_type == AssetType.MODEL:
            return self.model
        elif self.asset_type == AssetType.SUBSTANCE:
            raise NotImplementedError
        elif self.asset_type == AssetType.TEXTURE:
            return self.texture
        else:
            raise TypeError

    def update(self, asset_data_new, purge_maps: bool = False) -> None:
        """Updates asset data from another asset data,
        which may only be partially filled.

        Args:
        asset_data_new: Type AssetData, the instance to update from
        purge_maps: If True any existing map entries will be thrown away
        """

        self.display_name = _cond_set(asset_data_new.display_name,
                                      self.display_name)
        # self.asset_id is not meant to be changed
        # self.type is not meant to be changed
        # self.asset_name is not meant to be changed
        self.categories = _cond_set(asset_data_new.categories, self.categories)
        self.url = _cond_set(asset_data_new.url, self.url)
        self.slug = _cond_set(asset_data_new.slug, self.slug)
        self.thumb_urls = _cond_set(asset_data_new.thumb_urls, self.thumb_urls)
        self.published_at = _cond_set(asset_data_new.thumb_urls,
                                      self.thumb_urls)
        self.is_local = _cond_set(asset_data_new.is_local, self.is_local)
        self.downloaded_at = _cond_set(asset_data_new.downloaded_at,
                                       self.downloaded_at)
        self.is_purchased = _cond_set(asset_data_new.is_purchased,
                                      self.is_purchased)
        self.purchased_at = _cond_set(asset_data_new.purchased_at,
                                      self.purchased_at)
        self.render_custom_schema = _cond_set(
            asset_data_new.render_custom_schema, self.render_custom_schema)

        self.get_type_data().update(asset_data_new.get_type_data(), purge_maps)

    def flush_local(self):
        """Resets all information about local files."""

        self.is_local = False
        self.get_type_data().flush_local()

    def _convert_timestamp(self, ts: Optional[str]) -> str:
        """Returns formatted time from timestamp in timezone UTC."""

        if ts is None:
            return str(ts)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        time_formatted = dt.isoformat()
        return time_formatted

    def get_purchase_time(self) -> str:
        """Returns time of purchase as string."""

        return self._convert_timestamp(self.purchased_at)

    def get_download_time(self) -> str:
        """Returns time of download as string."""

        return self._convert_timestamp(self.downloaded_at)

    def is_backplate(self) -> bool:
        """Returns True, if the asset is a backplate/backdrop."""

        if self.asset_type != AssetType.TEXTURE:
            return False
        asset_name_low = self.asset_name.lower()
        return any(
            asset_name_low.startswith(vS) for vS in ["backdrop", "backplate"])

    def get_current_size(self, size_pref: str, local_only: bool) -> str:
        """Returns a (optionally) stored size, e.g. to use for the main import
        button after download of a non-default size. If no stored size is
        available, the closest size (based on availability and locality) will
        be returned.
        """

        size_current = self.runtime.get_current_size()
        size = size_current if size_current is not None else size_pref

        asset_type_data = self.get_type_data()
        if asset_type_data is not None:
            size = asset_type_data.get_size(size, local_only)

        return size


# Currently constants are defined here at the end,
# as some require above classes to be defined.
API_TYPE_TO_ASSET_TYPE = {"Brushes": AssetType.BRUSH,
                          "HDRS": AssetType.HDRI,
                          "Models": AssetType.MODEL,
                          "Substances": AssetType.SUBSTANCE,
                          "Textures": AssetType.TEXTURE,
                          }

ASSET_TYPE_TO_CATEGORY_NAME = {AssetType.BRUSH: "Brushes",
                               AssetType.HDRI: "HDRIs",
                               AssetType.MODEL: "Models",
                               AssetType.SUBSTANCE: "Substances",
                               AssetType.TEXTURE: "Textures",
                               }

MAPS_TYPE_NAMES = MapType.__members__
