import { Query, Listing, Schema, ThingDataNode, Config } from "@notabug/peer"
import NabIndexer from "."

const { ListingSort, ListingNode, ListingSpec } = Listing

export async function getListings(scope: any, thingId: string) {
  if (!thingId) return []
  const listings: string[] = []

  const [data, scores] = await Promise.all([
    Query.thingData(scope, thingId),
    Query.thingScores(scope, thingId)
  ])

  if (!data) return []

  const kind = ThingDataNode.kind(data)
  const authorId = ThingDataNode.authorId(data)
  const topic = ThingDataNode.topic(data)
    .trim()
    .toLowerCase()

  if (kind === "submission") {
    const domain = ThingDataNode.domain(data)
    const commands = (scores && scores.commands) || {}
    const taggedBy = []

    for (let key in commands) {
      if (key !== "anon") taggedBy.push(key)
    }

    if (topic) listings.push(`/t/${topic}`)
    if (topic !== "all") listings.push("/t/all")
    if (domain) listings.push(`/domain/${domain}`)
    if (authorId) {
      listings.push(`/user/${authorId}/submitted`)
      listings.push(`/user/${authorId}/overview`)
    }

    taggedBy.forEach(tagAuthorId =>
      listings.push(`/user/${tagAuthorId}/commented`)
    )
  } else if (kind === "comment") {
    const opId = ThingDataNode.opId(data)
    const replyToId = ThingDataNode.replyToId(data)
    const isCommand = ThingDataNode.isCommand(data)

    if (opId) listings.push(`/things/${opId}/comments`)
    if (topic) listings.push(`/t/comments:${topic}`)
    if (topic !== "all") listings.push("/t/comments:all")

    if (replyToId) {
      const replyToThingData = await Query.thingData(scope, replyToId)
      const replyToAuthorId = ThingDataNode.authorId(replyToThingData)

      if (replyToAuthorId) {
        const replyToKind = ThingDataNode.kind(replyToThingData)
        listings.push(`/user/${replyToAuthorId}/replies/overview`)
        if (replyToKind === "submission") {
          listings.push(`/user/${replyToAuthorId}/replies/submitted`)
        } else if (replyToKind === "comment") {
          listings.push(`/user/${replyToAuthorId}/replies/comments`)
        }
      }
    }

    if (authorId) {
      listings.push(`/user/${authorId}/comments`)
      listings.push(`/user/${authorId}/overview`)
      if (isCommand) listings.push(`/user/${authorId}/commands`)
      // TODO: update commented
    }
  } else if (kind === "chatmsg") {
    if (topic) listings.push(`/t/chat:${topic}`)
    if (topic !== "all") listings.push("/t/chat:all")
  }

  return listings
}

export async function describeThingId(scope: any, thingId: string) {
  if (!thingId) return null
  const spec = ListingSpec.fromSource("")
  const includes: string[] = await getListings(scope, thingId)
  if (!includes.length) return null

  return {
    id: thingId,
    includes,
    sorts: await Promise.all(
      Object.keys(ListingSort.sorts).map(async name => [
        name,
        await ListingSort.sorts[name](scope, thingId, spec)
      ])
    )
  }
}

export const descriptionToListingMap = (declarativeUpdate: any) => {
  const id = (declarativeUpdate && declarativeUpdate.id) || ""
  const includes = (declarativeUpdate && declarativeUpdate.includes) || []
  const sorts: [string, number][] =
    (declarativeUpdate && declarativeUpdate.sorts) || []
  const results = []

  for (let i = 0; i < includes.length; i++) {
    const listing = includes[i]

    for (let j = 0; j < sorts.length; j++) {
      const [sortName, value] = sorts[j]

      results.push([`${listing}/${sortName}`, [[id, value]]])
    }
  }

  return results
}

export async function indexThing(peer: NabIndexer, id: string) {
  const startedAt = new Date().getTime()
  const scope = peer.newScope()

  try {
    const description = await describeThingId(scope, id)
    const listingMap: any[] = descriptionToListingMap(description)

    const putData: any = {}

    const souls = listingMap.map(item => {
      const [listingPath]: [string, [string, number][]] = item
      return ListingNode.soulFromPath(Config.tabulator, listingPath)
    })

    if (!souls.length) {
      console.log("no souls", id, listingMap)
    }

    const nodes = {}

    await Promise.all(
      souls.map(soul =>
        scope.get(soul).then(node => {
          nodes[soul] = node
        })
      )
    )

    await Promise.all(
      listingMap.map(async item => {
        const [listingPath, updatedItems]: [string, [string, number][]] = item
        const soul = ListingNode.soulFromPath(Config.tabulator, listingPath)
        const existing = nodes[soul]
        const diff = await ListingNode.diff(existing, updatedItems, [])

        if (!diff) return
        putData[listingPath] = {
          _: {
            "#": soul
          },
          ...diff
        }
      })
    )

    if (Object.keys(putData).length) {
      const listingsSoul = Schema.ThingListingsMeta.route.reverse({
        thingId: id,
        tabulator: Config.tabulator
      })
      if (listingsSoul) {
        await new Promise(ok => peer.get(listingsSoul).put(putData, ok))
      }
    }
  } catch (e) {
    console.error("Indexer error", e.stack || e)
  } finally {
    scope.off()
  }

  const endedAt = new Date().getTime()
  console.log("indexed", (endedAt - startedAt) / 1000, id)
}

export function idsToIndex(msg: any) {
  const ids = []
  const put = msg && msg.put
  if (!put) return ids

  for (let soul in put) {
    const thingMatch = Schema.Thing.route.match(soul)
    const countsMatch = Schema.ThingVoteCounts.route.match(soul)
    if (countsMatch && countsMatch.tabulator !== Config.tabulator) continue
    const thingId = (thingMatch || countsMatch || {}).thingId || ""

    if (thingId && ids.indexOf(thingId) === -1) {
      ids.push(thingId)
    }
  }

  return ids
}
