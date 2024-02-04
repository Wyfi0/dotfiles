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
from typing import Optional

try:
    import ConfigParser
except Exception:
    import configparser as ConfigParser


class PoliigonEnvironment():
    """Poliigon environment used for assisting in program control flow."""

    addon_name: str  # e.g. poliigon-addon-blender
    base: str  # Path to base directory of addon or package
    env_filename: str

    config: ConfigParser.ConfigParser = None

    # Required env fields
    api_url: str = ""
    api_url_v2: str = ""
    env_name: str = ""

    required_attrs = ["api_url", "api_url_v2", "env_name"]

    # Optional env fields
    host: str = ""
    forced_sampling: bool = False
    local_updater_json: Optional[str] = None

    def __init__(self,
                 addon_name: str,
                 base: str = os.path.dirname(os.path.abspath(__file__)),
                 env_filename: str = "env.ini"):
        self.addon_name = addon_name
        self.base = base
        self.env_filename = env_filename
        self._update_files(base)
        self._load_env(base, env_filename)

    def _load_env(self, path, filename):
        env_file = os.path.join(path, filename)
        if os.path.exists(env_file):
            try:
                # Read .ini here and set values
                # https://docs.python.org/3/library/configparser.html#configparser.ConfigParser.optionxform
                config = ConfigParser.ConfigParser()
                config.optionxform = str
                config.read(env_file)

                # Required fields
                self.config = config
                self.api_url = config.get("DEFAULT", "api_url", fallback="")
                self.api_url_v2 = config.get("DEFAULT", "api_url_v2", fallback="")
                self.env_name = config.get("DEFAULT", "env_name", fallback="")

                for k, v in vars(self).items():
                    if k in self.required_attrs and v in [None, ""]:
                        raise ValueError(
                            f"Attribute '{k}' missing from env file")

                # Optional fields that should always be present
                self.host = config.get("DEFAULT", "host", fallback="")
                self.forced_sampling = config.getboolean(
                    "DEFAULT", "forced_sampling", fallback=False)
                self.local_updater_json = config.get(
                    "DEFAULT", "local_updater_json", fallback=None)

            except ValueError as e:
                msg = f"Could not load environment file for {self.addon_name}"
                raise RuntimeError(msg) from e
        else:
            # Assume production environment and set fallback values
            self.api_url = "https://api.poliigon.com/api/v1"
            self.api_url_v2 = "https://api.poliigon.com/api/v2"
            self.env_name = "prod"
            self.host = ""
            self.forced_sampling = False

    def _update_files(self, path):
        """Updates files in the specified path within the addon."""
        update_key = "_update"
        search_key = "env" + update_key
        files_to_update = [f for f in os.listdir(path)
                           if os.path.isfile(os.path.join(path, f))
                           and os.path.splitext(f)[0].endswith(search_key)]

        for f in files_to_update:
            f_split = os.path.splitext(f)
            tgt_file = f_split[0][:-len(update_key)] + f_split[1]

            try:
                os.replace(os.path.join(path, f), os.path.join(path, tgt_file))
                print(f"Updated {tgt_file}")
            except PermissionError as e:
                print(f"Encountered 'file_permission_error': {e}")
            except OSError as e:
                print(f"Encountered 'os_error': {e}")
