'use strict';

const os = require('os');
const { v4: uuidv4 } = require('uuid');
const node_path = require('path');
const { Storage } = require('@google-cloud/storage');
const { IdempotencyStrategy } = require('@google-cloud/storage/build/src/storage');
const { PubSub } = require('@google-cloud/pubsub');
const sleep = require('./sleep');

module.exports = {
    set_config,
    get_bucket,
    get_topic,
    get_subscription,
    upload_file,
    download_file,
    list_files,
    file_exists,
    delete_file,
    publish_to_topic,
    pull_from_subscription,
};

let gcp_cfg;

let storage, pubsub;

// https://github.com/googleapis/nodejs-storage/blob/main/samples/configureRetries.js
//
const retryOptions = {
    autoRetry: true,
    retryDelayMultiplier: 3,
    totalTimeout: 300,
    maxRetryDelay: 60,
    maxRetries: 5,
    idempotencyStrategy: IdempotencyStrategy.RetryAlways,
};

/*
cfg = {
    keyFilename: node_path.join(__dirname, 'gcp-key.json')
}
*/

function set_config(cfg) {
    gcp_cfg = cfg;
}

function get_bucket(bucket_name) {
    if (!storage) storage = new Storage({...gcp_cfg, retryOptions});
    return storage.bucket(bucket_name);
}

function get_topic(topic_name) {
    if (!pubsub) pubsub = new PubSub(gcp_cfg);
    return pubsub.topic(topic_name);
}

function get_subscription(name, options = {flowControl: { maxMessages: 32 }}) {
    if (!pubsub) pubsub = new PubSub(gcp_cfg);
    return pubsub.subscription(name, options);
}

async function publish_to_topic(topic_name, data, retries = 3) {
    const topic = get_topic(topic_name);
    const buffer = Buffer.from(JSON.stringify(data));
    for (let i = 0; i < retries; i++) {
        try {
            await topic.publishMessage({data: buffer});
            return true;
        } catch(err) {
            console.error(err.message);
            if (await handle_exception(err, {topic_name})) {
                continue
            } else {
                return false;
            }
        }
    }
    throw new Error(`failed to publish to topic to ${topic_name}`);
}

async function pull_from_subscription(name, max_messages = 64, timeout = 1000) {
    return new Promise((resolve) => {
        const options = {flowControl: { maxMessages: max_messages }}
        const subscription = get_subscription(name, options);
        const messages = [];
        subscription.on('message', (message) => {
            try {
                messages.push(JSON.parse(message.data.toString()));
            } catch(err) {
                console.error(err);
            } finally {
                message.ack();
            }
        });
        let i = 0;
        const timer = setInterval(() => {
            if (i >= timeout || messages.length >= max_messages) {
                subscription.close();
                resolve(messages);
                clearInterval(timer);
            }
            i += 250;
        }, 250);
    });
}

async function upload_file(local_filepath, bucket_name, path_key) {
    const bucket = get_bucket(bucket_name);
    const options = {destination: path_key, resumable: false};
    await bucket.upload(local_filepath, options);
    return true;
}

async function download_file(bucket_name, path_key, local_filepath) {
    const bucket = get_bucket(bucket_name);
    if (!local_filepath) {
        const filename = path_key.split('/').pop();
        local_filepath = node_path.join(os.tmpdir(), `${uuidv4()}-${filename}`);
    }
    const options = {destination: local_filepath, resumable: false};
    if (!local_filepath) local_filepath = `${os.tmpdir()}/${uuidv4()}-${path_key.split('/').pop()}`;
    try {
        await bucket.file(path_key).download(options);
        return {local_filepath};
    } catch(err) {
        if (err && err.code && err.code === 404) {
            return {};
        }
        throw err;
    }
}

async function list_files(bucket_name, path_key) {
    const bucket = get_bucket(bucket_name);
    try {
        const [files] = await bucket.getFiles({ prefix: path_key });
        return files.map(x => get_file_info(x));
    } catch(err) {
        if (err && err.code && err.code === 404) {
            return [];
        }
        throw err;
    }
}

async function file_exists(bucket_name, path_key, minima_size = 1024 * 8) {
    const bucket = get_bucket(bucket_name);
    try {
        const [files] = await bucket.getFiles({ prefix: path_key });
        if (files.length === 0) {
            return false;
        } else {
            for (let file of files) {
                const { name, size } = get_file_info(file);
                if (name === path_key) {
                    if (size < minima_size) return false;
                    else return true;
                }
            }
            return false;
        }
    } catch(err) {
        if (err && err.code && err.code === 404) {
            return false;
        }
        throw err;
    }
}

async function delete_file(bucket_name, path_key) {
    const bucket = get_bucket(bucket_name);
    try {
        await bucket.file(path_key).delete();
        return true;
    } catch(err) {
        if (err && err.code && err.code === 404) {
            return true;
        }
        throw err;
    }
}

function get_file_info({name, metadata: {size, timeCreated, updated}}) {
    return {
        name: name, size: Number(size), created_at: new Date(timeCreated), updated_at: new Date(updated)
    };
}

async function handle_exception(err, info) {
    if (err && err.code) {
        if  (err.code === 404) {
            return false;
        }
        if (err.code === 408 || err.code === 429 || err.code >= 500) {
            const secs = Math.floor(Math.random()*30) + 1;
            console.error(`error code ${err.code}, retry after ${secs} seconds`);
            await sleep(secs * 1000);
            return true;
        }
    } else {
        throw new Error(`unexpected, ${info ? 'info'  + JSON.stringify(info) + ', ' : ''}error without code: ${err.message}`);
    }
}