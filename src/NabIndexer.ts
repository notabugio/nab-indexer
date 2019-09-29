import { ChainGunSear, GunGraph, GunProcessQueue } from "@notabug/chaingun-sear"
import SocketClusterGraphConnector from "@notabug/chaingun-socket-cluster-connector"
import { Oracle, Query, Config } from "@notabug/peer"
import { idsToIndex, indexThing } from "./functions"

interface Opts {
  socketCluster: any
}

const DEFAULT_OPTS: Opts = {
  socketCluster: {
    hostname: process.env.GUN_SC_HOST || "localhost",
    port: process.env.GUN_SC_PORT || "4444",
    autoReconnect: true,
    autoReconnectOptions: {
      initialDelay: 1,
      randomness: 100,
      maxDelay: 500
    }
  }
}

Config.update({
  indexer: process.env.GUN_SC_PUB,
  tabulator: process.env.NAB_TABULATOR || process.env.GUN_SC_PUB
})

export class NabIndexer extends ChainGunSear {
  socket: SocketClusterGraphConnector
  oracle: Oracle
  indexerQueue: GunProcessQueue

  gun: ChainGunSear // temp compatibility thing for notabug-peer transition

  constructor(options = DEFAULT_OPTS) {
    const { socketCluster: scOpts, ...opts } = {
      ...DEFAULT_OPTS,
      ...options
    }

    const graph = new GunGraph()
    const socket = new SocketClusterGraphConnector(options.socketCluster)
    graph.connect(socket as any)
    graph.opt({ mutable: true })

    super({ graph, ...opts })
    this.gun = this

    this.indexerQueue = new GunProcessQueue()
    this.indexerQueue.middleware.use(id => indexThing(this, id))

    this.socket = socket
    this.authenticateAndListen()
  }

  newScope(): any {
    return Query.createScope(this, { unsub: !!process.env.NAB_INDEXER_UNSUB })
  }

  didReceiveDiff(msg: any) {
    this.socket.ingest([msg])
    const ids = idsToIndex(msg)
    if (ids.length) {
      this.indexerQueue.enqueueMany(ids)
    }
    this.indexerQueue.process()
  }

  authenticateAndListen() {
    if (process.env.GUN_SC_PUB && process.env.GUN_SC_PRIV) {
      this.socket
        .authenticate(process.env.GUN_SC_PUB, process.env.GUN_SC_PRIV)
        .then(() => {
          console.log(`Logged in as ${process.env.GUN_SC_PUB}`)
        })
        .catch(err => console.error("Error logging in:", err.stack || err))
    }

    if (process.env.GUN_ALIAS && process.env.GUN_PASSWORD) {
      this.user()
        .auth(process.env.GUN_ALIAS, process.env.GUN_PASSWORD)
        .then(() => {
          console.log(`Logged in as ${process.env.GUN_ALIAS}`)
          this.socket.subscribeToChannel(
            "gun/put/diff",
            this.didReceiveDiff.bind(this)
          )
        })
    }
  }
}
