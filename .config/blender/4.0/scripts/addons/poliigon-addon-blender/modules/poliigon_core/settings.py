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

try:
    import ConfigParser
except Exception:
    import configparser as ConfigParser


class PoliigonSettings():
    """Settings used for the addon."""

    addon_name: str  # e.g. poliigon-addon-3dsmax
    base: str  # Path to base directory of addon or package    
    software_source: str  # e.g. blender
    settings_filename: str

    config: ConfigParser.ConfigParser = None

    def __init__(self,
                 addon_name: str,
                 software_source: str,
                 base: str = os.path.join(os.path.expanduser("~"), "Poliigon"),
                 settings_filename: str = "settings.ini"):
        self.addon_name = addon_name
        self.base = os.path.join(base, software_source)
        self.settings_filename = settings_filename
        self.get_settings()

    def _ensure_sections_exist(self):
        sections = [
            "download",
            "library",
            "update",
            "logging",
            "purchase",
            "import",
            "onboarding",
            "user"
        ]
        added_sections = [
            self.config.add_section(sec)
            for sec in sections
            if not self.config.has_section(sec)
        ]

    def _populate_default_settings(self):
        self._ensure_sections_exist()

        defaults = {
            "download": {
                "brush": "2K",
                "download_lods": "true",
                "hdri_bg": "8K",
                "hdri_light": "1K",
                "lod": "NONE",
                "model_res": "NONE",
                "tex_res": "2K"
            },
            "library": {
                "primary": "",
                "add": ""
            },
            "logging": {
                "reporting_opt_in": "true",
                "verbose_logs": "true"
            },
            "purchase": {
                "auto_download": "true"
            },
            "user": {
                "token": "",
                "first_local_asset": ""
            }
        }

        for section_name, section_defaults in defaults.items():
            for option, value in section_defaults.items():
                self.config.set(section_name, option, value)

    def get_settings(self):
        # https://docs.python.org/3/library/configparser.html#configparser.ConfigParser.optionxform
        self.config = ConfigParser.ConfigParser()
        self.config.optionxform = str

        self._populate_default_settings()

        settings_file = os.path.join(self.base, self.settings_filename)
        if os.path.exists(settings_file):
            try:
                self.config.read(settings_file)
            except ValueError as e:
                print(f"Could not load settings for {self.addon_name}!")

    def save_settings(self):
        if self.config is None:
            print(f"No settings found for {self.addon_name}! Initializing...")
            self.get_settings()

        if not os.path.exists(self.base):
            try:
                os.makedirs(self.base)
            except Exception as e:
                print("Failed to create directory: ", e)
                raise

        settings_file = os.path.join(self.base, self.settings_filename)
        with open(settings_file, "w+") as f:
            self.config.write(f)
