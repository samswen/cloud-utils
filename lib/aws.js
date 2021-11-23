'use strict';

const fs = require('fs');
const os = require('os');
const { v4: uuidv4 } = require('uuid');
const node_path = require('path');
const { LambdaClient, InvokeCommand } = require ('@aws-sdk/client-lambda');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { SQSClient, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand, 
    SendMessageBatchCommand, DeleteMessageBatchCommand } = require('@aws-sdk/client-sqs');
const { S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand, ListObjectsCommand, 
    DeleteObjectCommand} = require('@aws-sdk/client-s3');
const get_check_sum = require('./get-check-sum');

module.exports = {
    set_config,
    get_s3_client,
    get_sns_client,
    get_sqs_client,
    get_lambda_client,
    upload_file,
    download_file,
    list_files,
    file_exists,
    delete_file,
    publish_notification,
    send_sqs_message,
    send_sqs_message_batch,
    get_sqs_messages,
    delete_sqs_message,
    delete_sqs_message_batch,
    invoke_lambda,
};

let s3_client, sns_client, sqs_client, lambda_client;

let aws_cfg = { region: 'us-east-1' };

/*
cfg = {
    account: ...,
    region: 'us-east-1',
    accessKeyId: ...,
    secretAccessKey: ...,
};
*/

function set_config(cfg) {
    aws_cfg = cfg;
}

function get_s3_client() {
    if (!s3_client) s3_client = new S3Client(aws_cfg);
    return s3_client;
}

function get_sns_client() {
    if (!sns_client) sns_client = new SNSClient(aws_cfg);
    return sns_client;
}

function get_sqs_client() {
    if (!sqs_client) sqs_client = new SQSClient(aws_cfg);
    return sqs_client;
}

function get_lambda_client() {
    if (!lambda_client) lambda_client = new LambdaClient(aws_cfg);
    return lambda_client;
}

async function upload_file(local_filepath, bucket_name, path_key) {
    const params = { Bucket: bucket_name, Key: path_key,
        Body: fs.createReadStream(local_filepath),
    };
    const response = await get_s3_client().send(new PutObjectCommand(params));
    if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
        return true;
    } else {
        return false;
    }
}

async function download_file(bucket_name, path_key, local_filepath) {
    if (!local_filepath) {
        const filename = path_key.split('/').pop();
        local_filepath = node_path.join(os.tmpdir(), `${uuidv4()}-${filename}`);
    }
    const params = { Bucket: bucket_name, Key: path_key };
    try {
        const response = await get_s3_client().send(new GetObjectCommand(params));
        if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
            await stream_to_file(response.Body, local_filepath);
            return { local_filepath };
        }
    } catch (err) {
        if (err && err.$metadata && err.$metadata.httpStatusCode === 404) return {};
        throw err;
    }
    throw new Error(`failed to download file from ${bucket_name}/${path_key}`);
}

async function list_files(bucket_name, path_key) {
    const params = { Bucket: bucket_name, Prefix: path_key };
    try {
        const response = await get_s3_client().send(new ListObjectsCommand(params));
        if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
            if (!response.Contents) return [];
            const files = response.Contents.map(({Key, LastModified, Size}) => {
                return {name: Key, updated_at: LastModified, size: Size};
            });
            return files;
        }
    } catch (err) {
        if (err && err.$metadata && err.$metadata.httpStatusCode === 404) return [];
        throw err;
    }
    throw new Error(`failed to list files for ${bucket_name}/${path_key}`);
}

async function file_exists(bucket_name, path_key, minima_size = 1024 * 8) {
    const params = { Bucket: bucket_name, Key: path_key };
    try {
        const response = await get_s3_client().send(new HeadObjectCommand(params));
        if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
            if (response.ContentLength < minima_size) {
                return false;
            } else {
                return true;
            }
        }
    } catch (err) {
        if (err && err.$metadata && err.$metadata.httpStatusCode === 404) return false;
        throw err;
    }
    throw new Error(`failed to check exists for ${bucket_name}/${path_key}`);
}

async function delete_file(bucket_name, path_key) {
    const params = { Bucket: bucket_name, Key: path_key };
    try {
        const response = await get_s3_client().send(new DeleteObjectCommand(params));
        if (response && response.$metadata && [200, 204].includes(response.$metadata.httpStatusCode)) {
            return true;
        }
    } catch (err) {
        if (err && err.$metadata && err.$metadata.httpStatusCode === 404) return true;
        throw err;
    }
    throw new Error(`failed to delete file for ${bucket_name}/${path_key}`);
}

async function publish_notification(topic_name, message, options) {
    const params = {TopicArn: get_topic_arn(topic_name), ...options};
    if (message.Message) Object.assign(params, message);
    else params.Message = JSON.stringify(message);
    if_fifo_message(params, topic_name, params.Message);
    const response = await get_sns_client().send(new PublishCommand(params));
    if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
        return true;
    } else {
        return false;
    }
}

async function send_sqs_message(queue_name, message, options) {
    const params = {QueueUrl: get_queue_url(queue_name), ...options};
    if (message.MessageBody) Object.assign(params, message);
    else params.MessageBody = JSON.stringify(message);
    if_fifo_message(params, queue_name, params.MessageBody);
    const response = await get_sqs_client().send(new SendMessageCommand(params));
    if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
        return true;
    } else {
        return false;
    }
}

async function send_sqs_message_batch(queue_name, messages, options) {
    const params = {QueueUrl: get_queue_url(queue_name), Entries: []};
    const Entries = params.Entries;
    let i = 0;
    for (const message of messages) { 
        const entry = message.MessageBody ? 
            { Id: String(i++), ...message, ...options } : 
            { Id: String(i++), ...options};
        if (!entry.MessageBody) entry.MessageBody = JSON.stringify(message);
        if_fifo_message(entry, queue_name, entry.MessageBody);
        Entries.push(entry);
        if (Entries.length === 10) {
            const response = await get_sqs_client().send(new SendMessageBatchCommand(params));
            if (!response || !response.$metadata || response.$metadata.httpStatusCode !== 200) {
                throw new Error('send sqs messages in batch failed')
            }
            Entries.length = 0;
        }
    }
    if (Entries.length > 0) {
        const response = await get_sqs_client().send(new SendMessageBatchCommand(params));
        if (!response || !response.$metadata || response.$metadata.httpStatusCode !== 200) {
            throw new Error('send sqs messages in batch failed')
        }
    }
}

async function get_sqs_messages(queue_name, options) {
    const params = {QueueUrl: get_queue_url(queue_name), AttributeNames: [ 'SentTimestamp' ], ...options};
    if (!params.MaxNumberOfMessages) params.MaxNumberOfMessages = 10;
    if (!params.VisibilityTimeout) params.VisibilityTimeout = 60;
    if (!params.WaitTimeSeconds) params.WaitTimeSeconds = 10;
    const response = await get_sqs_client().send(new ReceiveMessageCommand(params));
    if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
        return response.Messages;
    } else {
        return null;
    }
}

async function delete_sqs_message(queue_name, handle) {
    const params = {QueueUrl: get_queue_url(queue_name), ReceiptHandle: handle};
    const response = await get_sqs_client().send(new DeleteMessageCommand(params));
    if (response && response.$metadata && response.$metadata.httpStatusCode === 200) {
        return true;
    } else {
        return false;
    }
}

async function delete_sqs_message_batch(queue_name, handles) {
    const params = {QueueUrl: get_queue_url(queue_name), Entries: []};
    Entries = params.Entries;
    let i = 0;
    for (const handle of handles) {
        Entries.push({Id: String(i++), ReceiptHandle: handle});
        if (Entries.length === 10) {
            const response = await get_sqs_client().send(new DeleteMessageBatchCommand(params));
            if (!response || !response.$metadata || response.$metadata.httpStatusCode !== 200) {
                throw new Error('delete sqs messages in batch failed')
            }
            Entries.length = 0;
        }
    }
    if (Entries.length > 0) {
        const response = await get_sqs_client().send(new DeleteMessageBatchCommand(params));
        if (!response || !response.$metadata || response.$metadata.httpStatusCode !== 200) {
            throw new Error('delete sqs messages in batch failed')
        }
    }
}

async function invoke_lambda(function_name, message, options) {
    const params = {...options};
    params.FunctionName = function_name;
    if (message) {
        params.ClientContext = Buffer.from(JSON.stringify(message)).toString('base64');
    }
    if (!params.InvocationType) {
        if (!params.ClientContext) params.InvocationType = 'Event';
        else params.InvocationType = 'RequestResponse';
    }
    const response = await this.get_lambda_client().send(new InvokeCommand(params));
    if (response && response.StatusCode < 400) {
        if (response.Payload && Array.isArray(response.Payload)) {
            return parse_lambda_payload(response.Payload);
        } else {
            return true;
        }
    } else {
        return false;
    }
}

function get_topic_arn(topic_name) {
    return topic_name.startsWith('arn:') ? topic_name : 
        `arn:aws:sns:${aws_cfg.region}:${aws_cfg.account}:${topic_name}`;
}

function get_queue_url(queue_name) {
    return queue_name.startsWith('https:') ? queue_name : 
        `https://sqs.${aws_cfg.region}.amazonaws.com/${aws_cfg.account}/${queue_name}`;
}

function if_fifo_message(params, name, message) {
    if (name.endsWith('.fifo')) {
        if (!params.MessageGroupId) params.MessageGroupId = name;
        if (!params.MessageDeduplicationId) params.MessageDeduplicationId = get_check_sum(message);
    }
}

function stream_to_file(stream, local_filepath) {
    const file_stream = fs.createWriteStream(local_filepath);
    new Promise((resolve, reject) => {
        stream.pipe(file_stream).
            on('error', reject).
            on('close', resolve(true));
    });
}

function parse_lambda_payload(payload) {
    const first_code = payload[0];
    const last_code = payload[payload.length - 1];
    if ((first_code === 34 && last_code === 34) || (first_code === 116 && last_code === 101)) {
        payload.shift();
        payload.pop();
        return String.fromCharCode(...payload);
    } else {
        const result = String.fromCharCode(...payload);
        if (isNaN(result)) return result;
        else return Number(result);
    }
}
