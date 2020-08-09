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
import pandas as pd
from types import SimpleNamespace
from astropy.time import Time, TimeDelta
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.message_director import MessageDirector
from lsst.dm.CatchupArchiver.efdcurator import EFDCurator
from lsst.dm.CatchupArchiver.catchupboard import Catchupboard

LOGGER = logging.getLogger(__name__)

class CatchupDirector(MessageDirector):
    def __init__(self, parent, name, config_filename, log_filename):
        super().__init__(parent, name, config_filename, log_filename)
        self.parent = parent 

        self._msg_actions = { 
            # archive_controller messages
            'NEW_CATCHUP_ARCHIVE_ITEM_ACK': self.process_new_item_ack,
            'ARCHIVE_HEALTH_CHECK_ACK': self.process_archiver_health_check_ack,

            # forwarder messages
            'ASSOCIATED_ACK': self.process_association_ack,
            'CATCHUP_FWDR_XFER_PARAMS_ACK': self.process_xfer_params_ack,
            'CATCHUP_FWDR_END_READOUT_ACK': self.process_fwdr_end_readout_ack,
            'CATCHUP_FWDR_HEADER_READY_ACK': self.process_header_ready_ack,
            'SCAN': self.process_scan,
            'SCAN_ACK': self.process_scan_ack,
        }

    def configure(self):
        super().configure()
        config = self.getConfiguration()
        root = config['ROOT']

        self.CAMERA_NAME = self.config_val(root, 'CAMERA_NAME')
        self.ARCHIVE_CONTROLLER_NAME = self.config_val(root, 
                'ARCHIVE_CONTROLLER_NAME')

        self.ASSOCIATION_KEY = self.config_val(root, 'ASSOCIATION_KEY')

        # queues
        self.ARCHIVE_CTRL_PUBLISH_QUEUE = self.config_val(root, 
                'ARCHIVE_CTRL_PUBLISH_QUEUE')
        self.ARCHIVE_CTRL_CONSUME_QUEUE = self.config_val(root, 
                'ARCHIVE_CTRL_CONSUME_QUEUE')
        self.TELEMETRY_QUEUE = self.config_val(root, 'TELEMETRY_QUEUE')

        # messages
        self.FWDR_HEALTH_CHECK_ACK = self.config_val(root,
                'FWDR_HEALTH_CHECK_ACK')
        self.FILE_INGEST_REQUEST = self.config_val(root,
                'FILE_INGEST_REQUEST')
        self.NEW_ARCHIVE_ITEM = self.config_val(root,
                'NEW_ARCHIVE_ITEM')
        self.FWDR_XFER_PARAMS = self.config_val(root,
                'FWDR_XFER_PARAMS')
        self.FWDR_END_READOUT = self.config_val(root,
                'FWDR_END_READOUT')
        self.FWDR_HEADER_READY = self.config_val(root,
                'FWDR_HEADER_READY')

        self.local_catchup_expiration = self.config_val(root, 
                'local_catchup_expiration')

        sal_topics = self.config_val(root, 'sal_topics')
        efd_name = self.config_val(root, 'efd')
        self.efd = EFDCurator(efd_name, sal_topics)

        self.catchup_jobs = 'catchup_jobs'
        self.daq_query_key = 'images'

        redis_host = self.config_val(root, 'REDIS_HOST')
        redis_db = self.config_val(root, 'ARCHIVER_REDIS_DB')
        self.catchupboard = Catchupboard(self._name, redis_db, redis_host)

    async def setup_consumers(self):
        # messages from ArchiverController
        self.archive_consumer = Consumer(self.base_broker_url, self.parent, 
                self.ARCHIVE_CTRL_PUBLISH_QUEUE, self.on_message)
        self.archive_consumer.start()

        # messages from forwarder
        self.forwarder_consumer = Consumer(self.base_broker_url, self.parent,
                self.forwarder_publish_queue, self.on_message)
        self.forwarder_consumer.start()

        # telemetry messages from forwarder
        self.telemetry_consumer = Consumer(self.base_broker_url,  self.parent, 
                self.TELEMETRY_QUEUE, self.on_message)
        self.telemetry_consumer.start()

    async def process_scan(self, msg):
        LOGGER.info(f'scan msg received: {msg}')

        d = {}
        d['MSG_TYPE'] = 'SCAN'
        d['MINUTES' ] = self.local_catchup_expiration
        d['KEY'] = self.daq_query_key
        d['REPLY_QUEUE'] = self.forwarder_publish_queue

        await self.publish_message(self.forwarder_consume_queue, d)
        LOGGER.info(f'published {d} to {self.forwarder_consume_queue}')

    async def process_scan_ack(self, msg):
        LOGGER.info(f'scan_ack received: {msg}')

        efd_df = await self.query_efd()
        daq_list = self.catchupboard.get_daq_images(self.daq_query_key)

        if not daq_list:
            LOGGER.warn(f'There is no images from daq catalog to catchup')
            # publish_msg
            return

        self.store_jobs(daq_list, efd_df)
        imgs = self.catchupboard.get_jobs(self.catchup_jobs)

        for img_id in imgs:
            await self.start_msg_sequence(img_id)
            break

    def query_efd(self):
        end_time = Time.now()
        start_time = Time.now() - TimeDelta(60 * self.local_catchup_expiration, 
                format='sec')

        LOGGER.info(f'Querying efd for images from {start_time} to {end_time}')

        task = asyncio.create_task(self.efd.query_imageInOODS(
                start_time.tai, end_time.tai)) 
        return task

    def store_jobs(self, daq_imgs, efd_df):
        daq_imgs_list = []

        for img in daq_imgs:
            for location in self.wfs_ccd:
                for i in range(3):
                    rft = location[0:2]
                    sensor = location[-1] + str(i)

                    daq_imgs_list.append(
                        (img, rft, sensor, 0)
                    )

        daq_df = pd.DataFrame(daq_imgs_list, columns=[ 'obsid', 'raft', 'sensor',
            'statusCode'])
        merged_df = daq_df.merge(efd_df, on=[ 'obsid', 'raft', 'sensor',
            'statusCode'], how='left', indicator=True)
        merged_df = merged_df[merged_df._merge == 'left_only']
        grouped_df = merged_df.groupby(['obsid'], as_index=False).sum()

        self.catchupboard.create_jobs(self.catchup_jobs, *grouped_df.obsid.tolist())
        LOGGER.info(f'Finished computing jobs and storing in {self.catchup_jobs}')

        self.catchupboard.remove_daq_images(self.daq_query_key)
        LOGGER.info(f'Safely deleted {self.daq_query_key} from redis')

    async def process_xfer_params_ack(self, msg):
        await super().process_xfer_params_ack(msg)

        # msg = await self.efd.query_endReadout(img_id)
        d = {}
        d['imageName'] = msg['IMAGE_ID']
        d['imageIndex'] = 0
        d['imagesInSequence'] = 0
        d['imageDate'] = ''
        d['exposureTime'] = 0

        msg = SimpleNamespace(**d)
        await self.transmit_endReadout(msg)
        LOGGER.info(f'Sent message {d} to forwarder')

    async def process_fwdr_end_readout_ack(self, msg):
        await super().process_fwdr_end_readout_ack(msg)

        img_id = msg['IMAGE_ID']
        lfoa_df = await self.efd.query_largeFileObjectAvailable(img_id)

        if self.is_empty(lfoa_df):
            LOGGER.warn(f'No lfoa data in efd for this {img_id}')
            return

        d = {}
        d['id'] = img_id
        d['url'] = lfoa_df.iloc[0].url

        msg = SimpleNamespace(**d)
        await self.transmit_largeFileObjectAvailable(msg)
        LOGGER.info(f'Sent message {d} to forwarder')

    async def start_msg_sequence(self, img_id):
        # msg = await self.query_startIntegration(img_id)
        d = {}
        d['imageName'] = img_id
        d['imageIndex'] = 0
        d['imagesInSequence'] = 0
        d['imageDate'] = ''
        d['exposureTime'] = 0

        # initiated startIntegration. endReadout and lfoa follow after
        # startIntegration ack is received
        msg = SimpleNamespace(**d)
        await self.transmit_startIntegration(msg)
        LOGGER.info(f'Sent message {d} to forwarder')

    def is_empty(self, d):
        # if d is empty, return True
        if isinstance(d, dict):
            if d:
                return False 
            else:
                return True

        if isinstance(d, pd.DataFrame):
            if d.empty:
                return True
            else:
                return False
