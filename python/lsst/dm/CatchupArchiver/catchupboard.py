# This file is part of dm_CatchupArchiver
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from lsst.dm.csc.base.scoreboard import Scoreboard

class Catchupboard(Scoreboard):
    def __init__(self, device, db, host, port=6379):
        super().__init__(device, db, host, port)

    def get_daq_images(self, key):
        return self.conn.lrange(key, 0, -1)

    def remove_daq_images(self, key):
        self.conn.delete(key)

    def create_jobs(self, key, *values):
        if values:
            self.conn.sadd(key, *values)

    def get_jobs(self, key):
        return self.conn.smembers(key)
