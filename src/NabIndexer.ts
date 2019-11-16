import createAdapter from '@chaingun/node-adapters'
import { Config, GunProcessQueue, NotabugClient } from '@notabug/client'
import { idsToIndex, indexThing } from './functions'

Config.update({
  indexer: process.env.GUN_SC_PUB,
  tabulator: process.env.NAB_TABULATOR || process.env.GUN_SC_PUB
})

export class NabIndexer extends NotabugClient {
  public readonly indexerQueue: GunProcessQueue<any>

  constructor(...opts) {
    const dbAdapter = createAdapter()
    super(dbAdapter, ...opts)

    this.socket.socket.on('connect', this.authenticate.bind(this))
    this.indexerQueue = new GunProcessQueue()
    this.indexerQueue.middleware.use(id => indexThing(this, id))
    this.socket.subscribeToChannel(
      'gun/put/diff',
      this.didReceiveDiff.bind(this)
    )
  }

  protected didReceiveDiff(msg: any): void {
    const ids = idsToIndex(msg)
    if (ids.length) {
      // tslint:disable-next-line: readonly-array
      this.indexerQueue.enqueueMany(ids as string[])
    }
    this.indexerQueue.process()
  }
}
