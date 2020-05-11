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

import logging
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.message_director import MessageDirector

LOGGER = logging.getLogger(__name__)

class CatchupDirector(MessageDirector):
    def __init__(self, parent, name, config_filename, log_filename):
        super().__init__(parent, name, config_filename, log_filename)
        self.parent = parent
        self._msg_actions = { 
            'CATCHUP_HEALTH_CHECK_ACK': self.process_archiver_health_check_ack,
            'ASSOCIATED_ACK': self.process_association_ack
        }

    def configure(self):
        super().configure()
        config = self.getConfiguration()
        root = config['ROOT']

        self.ASSOCIATION_KEY = self.config_val(root, 'ASSOCIATION_KEY')

        self.ARCHIVE_CTRL_PUBLISH_QUEUE = self.config_val(root, 'ARCHIVE_CTRL_PUBLISH_QUEUE')
        self.ARCHIVE_CTRL_CONSUME_QUEUE = self.config_val(root, 'ARCHIVE_CTRL_CONSUME_QUEUE')
        self.TELEMETRY_QUEUE = self.config_val(root, 'TELEMETRY_QUEUE')

    async def setup_consumers(self):
        # messages from forwarder
        self.forwarder_consumer = Consumer(self.base_broker_url, self.parent,
                self.forwarder_publish_queue, self.on_message)
        self.forwarder_consumer.start()

    async def establish_connections(self, info):
        """ Establish non-CSC messaging connections
        """
        await self.setup_publishers()
        await self.setup_consumers()
