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

const indexName = "list";
const batchSize = 100;

const ids = [
  "d4f47bc1-c3e5-4e2b-990b-293bbf4358db",
  "461f7a7a-3418-41e0-8eec-f27a1f26d1e7",
  "17f83eae-ea86-485c-b545-8b9bcd3da021",
];

async function getDocumentsByIds(indexName, ids) {
  try {
    const query = {
      query: {
        terms: {
          id: ids,
        },
      },
      _source: ["id"],
    };
    const response = await esClient.search({
      index: indexName,
      body: query,
    });
    const hits = response.hits.hits;
    const docs = hits.map((hit) => hit._source);
    return docs;
  } catch (error) {
    console.error("Error querying Elasticsearch:", error);
  }
}

async function scanTableWithPagination() {
  let lastEvaluatedKey = null;
  do {
    const params = {
      TableName: tableName,
      ProjectionExpression: projectionExpression,
      ExclusiveStartKey: lastEvaluatedKey,
    };

    try {
      const data = await docClient.scan(params).promise();

      // Access the items from the response
      const items = data.Items;

      // Process the items as needed
      items.forEach((item) => {
        console.log(item);
      });

      // Update LastEvaluatedKey for the next iteration
      lastEvaluatedKey = data.LastEvaluatedKey;
    } catch (err) {
      console.error("Error scanning table:", err);
      throw err;
    }
  } while (lastEvaluatedKey);
  console.log("Scan completed.");
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

const start = async function () {
  const ddbIndexName = "resourceType-id-index";
  const tableName = process.env.AWS_DDB_TABLENAME;
  const ids = await queryResourceIdsWithPagination(
    tableName,
    ddbIndexName,
    "Encounter",
    10,
    null
  );
  console.log(ids);
  const esIndexName = "encounter";
  const docs = await getDocumentsByIds(esIndexName, ids);
  docs.forEach((doc) => {
    console.log(doc);
  });
};

start();
