import createAdapter from '@chaingun/node-adapters'
import {
  Config,
  createFederationAdapter,
  GunGraphAdapter,
  GunProcessQueue,
  NotabugClient
} from '@notabug/client'
import fs from 'fs'
import yaml from 'js-yaml'
import { idsToIndex, indexThing } from './functions'

Config.update({
  indexer: process.env.GUN_SC_PUB,
  tabulator: process.env.NAB_TABULATOR || process.env.GUN_SC_PUB
})

const PEERS_CONFIG_FILE = './peers.yaml'

let peersConfigTxt = ''

try {
  peersConfigTxt = fs.readFileSync(PEERS_CONFIG_FILE).toString()
} catch (e) {
  // tslint:disable-next-line: no-console
  console.warn('Peers config missing', PEERS_CONFIG_FILE, e.stack)
}

const peerUrls = yaml.safeLoad(peersConfigTxt) || []

export class NabIndexer extends NotabugClient {
  public readonly indexerQueue: GunProcessQueue<any>
  public readonly internalAdapter: GunGraphAdapter

  constructor(...opts) {
    const internalAdapter = createAdapter()
    const dbAdapter = createFederationAdapter(
      internalAdapter,
      peerUrls,
      internalAdapter,
      {
        putToPeers: true
      }
    )
    super(dbAdapter, ...opts)
    this.internalAdapter = internalAdapter

    this.socket.socket.on('connect', this.authenticate.bind(this))
    this.indexerQueue = new GunProcessQueue()
    this.indexerQueue.middleware.use(id => indexThing(this, id))
    this.socket.subscribeToChannel(
      'gun/put/diff',
      this.didReceiveDiff.bind(this)
    )
  }

  protected async didReceiveDiff(msg: any): Promise<void> {
    if (msg.put && !(await this.internalAdapter.put(msg.put))) {
      return // Avoid re-processing own writes
    }

    const ids = idsToIndex(msg)

    if (ids.length) {
      // tslint:disable-next-line: readonly-array
      this.indexerQueue.enqueueMany(ids as string[])
    }

    this.indexerQueue.process()
  }
}
