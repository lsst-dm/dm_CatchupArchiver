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

import asyncio
import logging
import pathlib
from lsst.dm.csc.base.archiver_csc import ArchiverCSC
from lsst.dm.CatchupArchiver.catchupdirector import CatchupDirector
from lsst.ts import salobj
from lsst.ts.salobj import State

LOGGER = logging.getLogger(__name__)

class CatchupArchiverCSC(ArchiverCSC):
    def __init__(self, schema_file, index, config_dir=None, 
            initial_state=salobj.State.STANDBY, initial_simulation_mode=0):
        schema_path = pathlib.Path(__file__).resolve().parents[4]\
                .joinpath("schema", schema_file)
        super().__init__("CatchupArchiver", 
                index=index, 
                schema_path=schema_path, 
                config_dir=config_dir,
                initial_state=initial_state, 
                initial_simulation_mode=initial_simulation_mode)

        self.director = CatchupDirector(self, "CatchupArchiver",
                "catchuparchiver_config.yaml", "CatchupArchiverCSC.log")
        self.director.configure()

        self.transitioning_to_fault_evt = asyncio.Event()
        self.transitioning_to_fault_evt.clear()
        self.current_state = None

        LOGGER.info("*********** Starting CatchupCArchiver *******************")

    @staticmethod
    def get_config_pkg():
        return "dm_config_catchup"
