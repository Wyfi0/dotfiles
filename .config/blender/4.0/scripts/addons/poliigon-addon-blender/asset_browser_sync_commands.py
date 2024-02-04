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
import hashlib
import json
import sys

from dataclasses import asdict, dataclass
from enum import IntEnum
from typing import Dict, Optional

# FILE_H2C belongs to proc (proc.stdin)
FILE_C2H = sys.stderr


CMD_MARKER_START = "POLIIGON_CMD_START\n"
CMD_MARKER_END = "POLIIGON_CMD_END\n"


class SyncCmd(IntEnum):
    """Command codes"""

    HELLO = 0        # Client -> Host
    HELLO_OK = 1     # Host -> Client (ack)
    ASSET = 2        # Host -> Client
    ASSET_OK = 3     # Client -> Host (ack)
    ASSET_ERROR = 4  # Client -> Host (ack)
    EXIT = 5         # Host -> Client
    EXIT_ACK = 6     # Client -> Host (ack)
    CMD_DONE = 7     # internal
    CMD_ERROR = 8    # both directions (ack)
    STILL_THERE = 9  # Host -> Client


@dataclass
class SyncAssetBrowserCmd():
    """Command to be transmitted between host and client"""

    code: SyncCmd
    data: Optional[Dict] = None
    params: Optional[Dict] = None
    checksum: Optional[str] = ""

    @classmethod
    def from_json(cls, buf: str):
        """Alternate constructor, used after receiving a command."""

        cmd_dict = json.loads(buf)
        if "code" not in cmd_dict:
            raise KeyError("code")
        new = cls(**cmd_dict)
        new.code = SyncCmd(new.code)

        cmd_is_ok = new.check_checksum()
        if not cmd_is_ok:
            raise RuntimeError("Checksum error")
        return new

    def check_checksum(self) -> bool:
        checksum_to_test = self.checksum
        self.checksum = ""
        cmd_dict = asdict(self)
        json_str = json.dumps(cmd_dict, indent=4, default=vars) + "\n"
        checksum_calculated = hashlib.md5(json_str.encode("utf-8")).hexdigest()
        return checksum_to_test == checksum_calculated

    def to_json(self) -> str:
        cmd_dict = asdict(self)
        json_str = json.dumps(cmd_dict, indent=4, default=vars) + "\n"
        return json_str

    def calc_checksum(self) -> None:
        self.checksum = ""
        json_str = self.to_json()
        try:
            self.checksum = hashlib.md5(json_str.encode("utf-8")).hexdigest()
        except Exception as e:
            print(f"MD5 error: {e}")

    def prepare_send(self) -> str:
        self.calc_checksum()
        json_str = CMD_MARKER_START
        json_str += self.to_json()
        json_str += CMD_MARKER_END
        return json_str

    def send_to_process(self, proc) -> None:  # use on host
        try:
            proc.stdin.write(self.prepare_send())
            proc.stdin.flush()
        except Exception:
            # Deliberately silencing exceptions here.
            # Any exceptions regarding unexpectedly closed handles are handled
            # in respective threads instead.
            pass

    def send_to_stdio(self, file=FILE_C2H) -> None:  # use on client
        file.write(self.prepare_send())
        file.flush()
