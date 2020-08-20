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
import pandas as pd
import logging
from lsst_efd_client import EfdClient

LOGGER = logging.getLogger(__name__)

class EFDCurator:
    def __init__(self, db, topics):
        try:
            self.db = EfdClient(db)
        except ValueError:
            err = f'efd with {db} name does not exist'
            LOGGER.error(err)
            raise Exception(err)

        LOGGER.info(f'Connected to efd at {db}')

        try:
            # sal topic names
            self.largeFileObjectAvailable = topics['largeFileObjectAvailable']
            self.endReadout = topics['endReadout']
            self.startIntegration = topics['startIntegration']
            self.imageInOODS = topics['imageInOODS']
        except KeyError as e:
            LOGGER.error(e)
            raise Exception(e)

    async def query_largeFileObjectAvailable(self, image_id):
        query = f'SELECT id, url FROM "{self.largeFileObjectAvailable}" WHERE id=\'{image_id}\''
        ret = await self.db._do_query(query)
        return ret

    async def query_startIntegration(self, image_id):
        query = f'SELECT * FROM "{self.startIntegration}" WHERE id=\'{image_id}\''
        ret = await self.db._do_query(query)
        return ret

    async def query_endReadout(self, image_id):
        query = f'SELECT * FROM "{self.endReadout}" WHERE id=\'{image_id}\''
        ret = await self.db._do_query(query)
        return ret

    async def query_imageInOODS(self, start, end):
        fields = ['obsid', 'raft', 'sensor', 'statusCode']
        ret = await self.db.select_time_series(self.imageInOODS, fields, start, end)
        return ret
