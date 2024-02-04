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
import time


def f_Ex(vPath):
    return os.path.exists(vPath)


def f_FName(vPath):
    return os.path.splitext(os.path.basename(vPath))[0]


def f_FExt(vPath):
    return os.path.splitext(os.path.basename(vPath))[1].lower()


def f_FNameExt(vPath):
    vSplit = list(os.path.splitext(os.path.basename(vPath)))
    vSplit[1] = vSplit[1].lower()
    return vSplit


def f_FSplit(vPath):
    vSplit = list(os.path.splitext(vPath))
    vSplit[1] = vSplit[1].lower()
    return vSplit


def f_MDir(vPath):
    if not f_Ex(vPath):
        try:
            os.makedirs(vPath)
        except Exception as e:
            print("Failed to create directory: ", e)


def timer(fn):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()

        result = fn(*args, **kwargs)

        end_time = time.perf_counter()
        duration = round(end_time - start_time, 2)
        if duration > 60:
            msec = str(duration - int(duration)).split('.')[1]
            duration = f"{time.strftime('%M:%S', time.gmtime(duration))}.{msec}"

        print(f"{fn.__name__} : {duration}s")

        return result
    return wrapper


def construct_model_name(asset_name, size, lod):
    """Constructs the model name from the given inputs."""
    if lod:
        model_name = f"{asset_name}_{size}_{lod}"
    else:
        model_name = f"{asset_name}_{size}"
    return model_name


def shorten_label(label: str, max_len: int) -> str:
    """Shortens the given label by replacing the center part with '...'"""

    if len(label) <= max_len:
        return label

    overlen = (len(label) - (max_len - 3)) // 2
    center = len(label) // 2
    return label[:center - overlen] + "..." + label[center + overlen:]
