#!/usr/bin/env python
import asyncio
from lsst.dm.CatchupArchiver.catchuparchiver_csc import CatchupArchiverCSC

csc = CatchupArchiverCSC(index=None, schema_file='CatchupArchiver.yaml', initial_simulation_mode=False)
asyncio.get_event_loop().run_until_complete(csc.done_task)
