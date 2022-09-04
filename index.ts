import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";


const table_name = 'waf_prod_acquisition_logs';
const bucket_log = 's3-waf-prod-acquisition-all-logs'

const topUserQuery =
    `with t1 as (
        SELECT
            httprequest[1].clientip clientip,
            httprequest[1].uri uri,
            httprequest[1].httpmethod httpmethod
        FROM ${table_name}
        WHERE
            httprequest[1].uri='/api/2/register' AND
            httprequest[1].httpmethod='POST'
        )

        SELECT
            t1.clientip, count(*) as cnt
        FROM t1 group by t1.clientip
        having count(*) >= 10
        order by cnt DESC`;

function createTableQuery() {
    return `CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
        httpRequest array<
            struct<clientIp: string,
                   uri: string,
                   httpMethod: string>>
    )
    PARTITIONED BY (
      day string
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION 's3://${bucket_log}/2021/10/';`;
}

function addPartitonQuery() {
    return `ALTER TABLE ${table_name} ADD IF NOT EXISTS
        PARTITION (day='16') LOCATION 's3://${bucket_log}/2021/10/16'
        PARTITION (day='17') LOCATION 's3://${bucket_log}/2021/10/17'
        PARTITION (day='18') LOCATION 's3://${bucket_log}/2021/10/18';`;
}

function getQueryUri(queryId: string) {
    const config = new pulumi.Config("aws");
    const region = config.require("region");
    return `https://${region}.console.aws.amazon.com/athena/home?force#query/saved/${queryId}`;
}

const athena_waf_db = new aws.athena.Database('prod_waf_logs', {
    bucket: 'aws-athena-query-results-123456789012-us-east-1',
    forceDestroy: true,
    name: 'prod_waf_logs'
});

const createTableAthenaQuery = new aws.athena.NamedQuery('create_waf_logs_table',
    { database: athena_waf_db.id, query: createTableQuery(), description: 'Create WAF logs table'});

const addPartitionAthenaQuery = new aws.athena.NamedQuery('add_waf_logs_partitions',
    { database: athena_waf_db.id, query: addPartitonQuery(), description: 'Add partitions to WAF logs table base on year, month, day'});

const topUserAthenaQuery = new aws.athena.NamedQuery('topUser',
    { database: athena_waf_db.id, query: topUserQuery, description: 'Run query to get data'});

export const createTableAthenaQueryUri = createTableAthenaQuery.id.apply(getQueryUri);
export const addPartitionAthenaQueryUri = addPartitionAthenaQuery.id.apply(getQueryUri);
export const topUserQueryUri = topUserAthenaQuery.id.apply(getQueryUri);