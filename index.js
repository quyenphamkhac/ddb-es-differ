const AWS = require("aws-sdk");
const elasticsearch = require("elasticsearch");
const fs = require("fs");
require("dotenv").config();

AWS.config.credentials = new AWS.SharedIniFileCredentials({
  profile: process.env.AWS_PROFILE,
});

AWS.config.update({ region: process.env.AWS_REGION });

const esClient = new elasticsearch.Client({
  host: process.env.AWS_ES_HOST,
  connectionClass: require("http-aws-es"),
  awsConfig: new AWS.Config({ credentials: AWS.config.credentials }),
});

const docClient = new AWS.DynamoDB.DocumentClient();

async function getDocumentsByIds(indexName, ids, limit) {
  try {
    const query = {
      query: {
        terms: {
          id: ids,
        },
      },
      from: 0,
      size: limit,
      _source: ["id"],
    };
    const response = await esClient.search({
      index: indexName,
      body: query,
    });
    const hits = response.hits.hits;
    const docIds = hits.map((hit) => hit._source.id);
    return docIds;
  } catch (error) {
    console.error("Error querying Elasticsearch:", error);
  }
}

async function queryResourceIdsWithPagination(
  tableName,
  indexName,
  resourceType,
  limitSize,
  lastEvaluatedKey
) {
  const params = {
    TableName: tableName,
    KeyConditionExpression: "#resourceType = :resourceType",
    ExpressionAttributeValues: {
      ":resourceType": resourceType,
    },
    ExpressionAttributeNames: {
      "#resourceType": "resourceType",
    },
    Limit: limitSize,
    IndexName: indexName,
    ExclusiveStartKey: lastEvaluatedKey, // Use LastEvaluatedKey for pagination
  };

  try {
    const data = await docClient.query(params).promise();

    return data;
  } catch (err) {
    console.error("Error querying table:", err);
  }
}

function getArrayDifference(array1, array2) {
  const set1 = new Set(array1);

  const difference = [];

  for (const item of array2) {
    if (!set1.has(item)) {
      difference.push(item);
    }
  }

  return difference;
}

async function updateMetaField(tableName, idValue, vidValue) {
  // Get the document by primary key
  const getParams = {
    TableName: tableName,
    Key: {
      id: idValue,
      vid: vidValue,
    },
    ProjectionExpression: "id, vid, meta",
  };

  try {
    const getResult = await docClient.get(getParams).promise();

    if (!getResult.Item) {
      console.error("Document not found");
      return;
    }

    console.log(getResult.Item);

    // Update the meta field with a new lastUpdated timestamp
    const newMeta = {
      ...getResult.Item.meta, // Keep the existing versionId
      lastUpdated: new Date().toISOString(),
    };
    console.log(newMeta);

    // Update the document with the new meta field
    const updateParams = {
      TableName: tableName,
      Key: {
        id: idValue,
        vid: vidValue,
      },
      UpdateExpression: "SET #meta = :newMeta",
      ExpressionAttributeNames: {
        "#meta": "meta",
      },
      ExpressionAttributeValues: {
        ":newMeta": newMeta,
      },
      ReturnValues: "NONE",
    };

    await docClient.update(updateParams).promise();
    console.log("Updated document:", idValue, vidValue);
  } catch (error) {
    console.error("Error updating document:", error);
  }
}

async function updateMetaFields(tableName, ids) {
  for (let index = 0; index < ids.length; index++) {
    const id = ids[index];
    await updateMetaField(tableName, id, 1);
  }
}

function writeIdsToFile(ids, filePath) {
  const dataToWrite = ids.join("\n") + "\n";

  fs.appendFile(filePath, dataToWrite, (err) => {
    if (err) {
      console.error("Error writing to file:", err);
    } else {
      console.log("IDs written to file successfully!");
    }
  });
}
const start = async function () {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.info("Please provide a FHIR resource type to run this script.");
    return;
  }
  const [resourceType] = args;
  console.info("FHIR Resource Type: ", resourceType);
  const limit = 2000;
  const ddbIndexName = process.env.AWS_DDB_INDEXNAME;
  const tableName = process.env.AWS_DDB_TABLENAME;
  let currentlastEvaluatedKey = null;
  // let currentlastEvaluatedKey = {
  //   id: "61c3edfe-f558-4d1a-971f-fdeeaf1e9646",
  //   vid: 1,
  //   resourceType: "Observation",
  // };
  let queryResponse = await queryResourceIdsWithPagination(
    tableName,
    ddbIndexName,
    resourceType,
    limit,
    currentlastEvaluatedKey
  );
  let ddbDocIds = queryResponse.Items.map((item) => item.id);
  console.log("DynamoDB documents: ", ddbDocIds.length);
  let lastEvaluatedKey = queryResponse.LastEvaluatedKey;
  let counter = 0;
  const esIndexName = resourceType.toLowerCase();
  const esDocIds = await getDocumentsByIds(esIndexName, ddbDocIds, limit);
  console.log("Elasticsearch documents: ", esDocIds.length);

  const diffIds = getArrayDifference(esDocIds, ddbDocIds);
  // Show missing data ids
  console.log("Missed ids: ", diffIds);
  const archiveFilePath = `archive/${resourceType}-diff.txt`;
  writeIdsToFile(diffIds, archiveFilePath);
  await updateMetaFields(tableName, diffIds);
  try {
    while (lastEvaluatedKey) {
      counter = counter + 1;
      console.log(`FHIR Resource Type ${resourceType}
        Step ${counter + 1} is excuting. 
        Total documents: ${counter * limit}
        LastEvaluatedKey: ${lastEvaluatedKey}`);
      console.log("LastEvaluatedKey", lastEvaluatedKey);
      queryResponse = await queryResourceIdsWithPagination(
        tableName,
        ddbIndexName,
        resourceType,
        limit,
        lastEvaluatedKey
      );
      ddbDocIds = queryResponse.Items.map((item) => item.id);
      console.log("DynamoDB documents: ", ddbDocIds.length);
      lastEvaluatedKey = queryResponse.LastEvaluatedKey;

      // Elasticsearch Index Name is FHIR Resource Type lowercase
      const esIndexName = resourceType.toLowerCase();
      const esDocIds = await getDocumentsByIds(esIndexName, ddbDocIds, limit);
      console.log("Elasticsearch documents: ", esDocIds.length);

      const diffIds = getArrayDifference(esDocIds, ddbDocIds);
      // Show missing data ids
      console.log("Missed ids: ", diffIds);
      const archiveFilePath = `archive/${resourceType}-diff.txt`;
      writeIdsToFile(diffIds, archiveFilePath);
      await updateMetaFields(tableName, diffIds);
    }
  } catch (error) {
    console.log("current Evaluated Key", lastEvaluatedKey);
    console.error(error);
  }
};

start();
