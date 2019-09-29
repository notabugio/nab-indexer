import { ChainGunSear, GunGraph } from "@notabug/chaingun-sear"
import SocketClusterGraphConnector from "@notabug/chaingun-socket-cluster-connector"
import { Oracle, Indexer, Query, Config } from "@notabug/peer"

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

  gun: ChainGunSear // temp compatibility thing for notabug-peer transition

  constructor(options = DEFAULT_OPTS) {
    const { socketCluster: scOpts, ...opts } = {
      ...DEFAULT_OPTS,
      ...options
    }

    const graph = new GunGraph()
    const socket = new SocketClusterGraphConnector(options.socketCluster)
    graph.connect(socket as any)

    super({ graph, ...opts })
    this.gun = this

    this.socket = socket
    this.authenticateAndListen()
  }

  /**
   * Temporary compatibility measure for notabug-peer until logic is moved here
   */
  newScope(opts?: any): any {
    return Query.createScope(this, opts)
  }

  /**
   * Temporary compatibility measure for notabug-peer until logic is moved here
   */
  isLoggedIn(): any {
    return this.user().is
  }

  buildOracle() {
    const oracle = new Oracle()
    oracle.use(new Indexer.Queue(this))
    return oracle
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
          this.oracle = this.buildOracle()
          this.socket.subscribeToChannel("gun/put/diff", msg => {
            this.oracle.onPut(msg)
          })
        })
    }
  }
}
