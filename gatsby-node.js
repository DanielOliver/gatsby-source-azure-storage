const crypto = require("crypto");
var azure = require('azure-storage');
var path = require('path');
var mkdirp = require('mkdirp');
var { createFileNode } = require('gatsby-source-filesystem/create-file-node');

const getValueWithDefault = (valueItem, defaultValue) => { return ((valueItem || { _: defaultValue })._ || defaultValue) }
const getValue = valueItem => getValueWithDefault(valueItem, null)


function makeTableNode(createNode, createNodeId, tableName, tableType) {
  const item = {
    name: tableName,
    type: tableType
  }
  const nodeId = createNodeId('azureTable/' + tableName)
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
      type: 'azureTable',
      content: nodeContent,
      contentDigest: nodeContentDigest,
    },
  })
  createNode(nodeData)
}

function makeContainerNode(createNode, createNodeId, containerName, localFolder) {
  const item = {
    name: containerName,
    localFolder: localFolder
  }
  const nodeId = createNodeId('azureContainer/' + containerName)
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
      type: 'azureContainer',
      content: nodeContent,
      contentDigest: nodeContentDigest,
    },
  })
  createNode(nodeData)
}

function downloadBlobFile(createNode, createNodeId, blobService, { container, name, localPath }) {
  mkdirp.sync(path.dirname(localPath, createNodeId));
  return new Promise(function (resolve, reject) {
    try {
      blobService.getBlobToLocalFile(container, name, localPath, function (error, result, response) {
        if (!error) {
          createFileNode(localPath, createNodeId, pluginOptions = {
            name: "gatsby-source-azure-storage"
          }).then(function (node) {

              let publicUrl = blobService.getUrl(container, name)
              let nodeWithUrl = Object.assign({ url: publicUrl}, node)
              createNode(nodeWithUrl)

              resolve()
            }, function (failure) {
              console.error(` Failed creating node from blob "${name}" from container "${container}"`)
              reject(failure)
            })
        } else {
          console.error(` Failed downloading blob "${name}" from container "${container}"`)
          reject(error)
        }
      })
    } catch (err) {
      console.error(` Failed to download blob "${name}" from container "${container}"`)
      reject(err)
    }
  })
}

function makeNodesFromQuery(createNode, createNodeId, tableService, tableName, typeName) {
  return new Promise(function (resolve, reject) {
    try {
      const query = new azure.TableQuery()

      function queryWithToken(token) {
        tableService.queryEntities(tableName, query, token, function (error, result, response) {
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

            if (result.continuationToken == null) {
              resolve()
            } else {
              queryWithToken(result.continuationToken)
            }
          } else {
            console.error(` Unable to query table "${tableName}"`)
            reject(error)
          }
        })
      }

      queryWithToken(null)
    } catch (err) {
      console.error(` Error on table "${tableName}"`)
      reject(err)
    }
  })
}

function makeNodesFromContainer(createNode, createNodeId, blobService, containerName, downloadFolder) {
  return new Promise(function (resolve, reject) {
    try {
      function queryWithToken(token = null, nodes = []) {
        blobService.listBlobsSegmented(containerName, token, function (error, result, response) {
          if (!error) {
            result.entries.forEach(value => {
              const item = {
                name: value.name,
                container: value.container || containerName,
                contentMD5: value.contentSettings.contentMD5,
                creationTime: value.creationTime,
                lastModified: value.lastModified,
                blobType: value.blobType,
                serverEncrypted: value.serverEncrypted,
                localPath: (downloadFolder == null ? null : path.join(process.cwd(), downloadFolder, value.name))
              }
              const nodeId = createNodeId(`${value.name}/${value.contentSettings.contentMD5}`)
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
                  type: 'azureBlob',
                  content: nodeContent,
                  contentDigest: nodeContentDigest,
                },
              })
              createNode(nodeData)

              nodes.push(nodeData)
            })

            if (result.continuationToken == null) {
              resolve(nodes)
            } else {
              queryWithToken(result.continuationToken, nodes)
            }
          } else {
            console.error(` Unable to query container "${tableName}"`)
            reject(error)
          }
        })
      }

      queryWithToken()
    } catch (err) {
      console.error(` Error on container "${containerName}"`)
      reject(err)
    }
  })
}

exports.sourceNodes = (
  { actions, createNodeId },
  configOptions
) => {
  const { createNode } = actions

  // Gatsby adds a configOption that's not needed for this plugin, delete it
  delete configOptions.plugins

  let tableService = azure.createTableService()
  let blobService = azure.createBlobService()

  let hasTables = configOptions.tables != null && configOptions.tables.length > 0
  let tablePromises = hasTables
    ? configOptions.tables.map(x => {
      let typeName = (x.type || x.name)
      makeTableNode(createNode, createNodeId, x.name, typeName)
      return makeNodesFromQuery(createNode, createNodeId, tableService, x.name, typeName)
    })
    : []

  let blobPromises = (configOptions.containers != null && configOptions.containers.length > 0)
    ? configOptions.containers.map(x => {
      let localFolder = (x.localFolder || configOptions.containerLocalFolder)
      makeContainerNode(createNode, createNodeId, x.name, localFolder)
      let promiseNode = makeNodesFromContainer(createNode, createNodeId, blobService, x.name, localFolder)

      if (localFolder == null) {
        return promiseNode
      } else {
        return promiseNode
          .then(values => {
            return Promise.all(values.map(node => {
              return downloadBlobFile(createNode, createNodeId, blobService, node)
            }))
          })
      }
    })
    : []

  return Promise.all(tablePromises.concat(blobPromises))
}