const AWS = require("aws-sdk");
const elasticsearch = require("elasticsearch");
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

    // Access the items from the response
    const items = data.Items;

    // Process the items as needed
    const ids = items.map((item) => {
      return item.id;
    });
    return ids;
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

const start = async function () {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.info("Please provide a FHIR resource type to run this script.");
    return;
  }
  const [resourceType] = args;
  console.info("FHIR Resource Type: ", resourceType);
  const limit = 100;
  const ddbIndexName = process.env.AWS_DDB_INDEXNAME;
  const tableName = process.env.AWS_DDB_TABLENAME;
  const ddbDocIds = await queryResourceIdsWithPagination(
    tableName,
    ddbIndexName,
    resourceType,
    limit,
    null
  );
  console.log("DDB", ddbDocIds.length);

  // Elasticsearch Index Name is FHIR Resource Type lowercase
  const esIndexName = resourceType.toLowerCase();
  const esDocIds = await getDocumentsByIds(esIndexName, ddbDocIds, limit);
  console.log("ES", esDocIds.length);

  const diffIds = getArrayDifference(esDocIds, ddbDocIds);
  // Show missing data ids
  console.log("DIFF", diffIds);
};

start();
