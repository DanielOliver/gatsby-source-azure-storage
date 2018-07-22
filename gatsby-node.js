const crypto = require("crypto");
var azure = require('azure-storage');

exports.sourceNodes = (
  { actions, createNodeId },
  configOptions
) => {
  const { createNode } = actions

  // Gatsby adds a configOption that's not needed for this plugin, delete it
  delete configOptions.plugins

  if (configOptions.tables == null || !Array.isArray(configOptions.tables)) {
    console.warn("Expected array of tables.")
    return null
  }

  var tableSvc = azure.createTableService()

  const getValueWithDefault = (valueItem, defaultValue) => { return ((valueItem || { _: defaultValue })._ || defaultValue) }
  const getValue = valueItem => getValueWithDefault(valueItem, null)

  function makeNodesFromQuery(tableName, typeName) {
    return new Promise(function (resolve, reject) {
      const query = new azure.TableQuery()
      tableSvc.queryEntities(tableName, query, null, function (error, result, response) {
        if (!error) {
          result.entries.forEach(value => {
            const item = Object.entries(value).reduce((o, prop) => ({ ...o, [prop[0]]: getValue(prop[1]) }), {})
            const nodeId = createNodeId(`${item.PartitionKey}/${item.RowKey}`)
            const nodeContent = JSON.stringify(item)
            const nodeContentDigest = crypto
              .createHash('md5')
              .update(nodeContent)
              .digest('hex')
            const nodeData = Object.assign(item, {
              id: nodeId,
              parent: null,
              children: [],
              internal: {
                type: typeName,
                content: nodeContent,
                contentDigest: nodeContentDigest,
              },
            })
            createNode(nodeData)
          })
          if (result.continuationToken != null) {
            response.continuationToken = result.continuationToken
          }
          resolve()
        } else {
          reject(error)
        }
      })
    })
  }
  return Promise.all(configOptions.tables.map(x => makeNodesFromQuery(x.name, (x.type || x.name))))
}