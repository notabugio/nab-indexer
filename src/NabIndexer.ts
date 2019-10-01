import { ChainGunSear, GunGraph, GunProcessQueue } from "@notabug/chaingun-sear"
import SocketClusterGraphConnector from "@notabug/chaingun-socket-cluster-connector"
import { Query, Config } from "@notabug/peer"
import { pubFromSoul, unpackNode } from "@notabug/gun-sear"
import { idsToIndex, indexThing } from "./functions"
import LmdbGraphConnector from "@notabug/chaingun-lmdb"

interface Opts {
  socketCluster: any
  lmdb?: any
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
  },
  lmdb: {
    path: process.env.LMDB_PATH || "./data",
    mapSize: 1024 ** 4
  }
}

Config.update({
  indexer: process.env.GUN_SC_PUB,
  tabulator: process.env.NAB_TABULATOR || process.env.GUN_SC_PUB
})

export class NabIndexer extends ChainGunSear {
  socket: SocketClusterGraphConnector
  lmdb: LmdbGraphConnector
  indexerQueue: GunProcessQueue

  gun: ChainGunSear // temp compatibility thing for notabug-peer transition

  constructor(options = DEFAULT_OPTS) {
    const { socketCluster: scOpts, lmdb: lmdbOpts, ...opts } = {
      ...DEFAULT_OPTS,
      ...options
    }

    console.log("lmdbopts", lmdbOpts)
    const graph = new GunGraph()
    const lmdb = new LmdbGraphConnector(lmdbOpts)
    lmdb.sendRequestsFromGraph(graph as any)
    const socket = new SocketClusterGraphConnector(options.socketCluster)
    graph.connect(lmdb as any)
    graph.opt({ mutable: true })

    socket.sendPutsFromGraph(graph as any)

    super({ graph, ...opts })
    this.gun = this
    this.lmdb = lmdb
    this.directRead = this.directRead.bind(this)

    this.indexerQueue = new GunProcessQueue()
    this.indexerQueue.middleware.use(id => indexThing(this, id))

    this.socket = socket
    this.authenticateAndListen()
  }

  newScope(): any {
    return Query.createScope(this, {
      getter: this.directRead,
      unsub: true
    })
  }

  directRead(soul: string) {
    return new Promise(ok => {
      this.lmdb.get({
        soul,
        cb: (msg: any) => {
          const node = (msg && msg.put && msg.put[soul]) || undefined
          if (pubFromSoul(soul)) unpackNode(node, "mutable")

          ok(node)
        }
      })
    })
  }

  didReceiveDiff(msg: any) {
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
