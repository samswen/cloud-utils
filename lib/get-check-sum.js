'use strict';

const base62 = require('base-x')('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ');
const crypto = require('crypto');

module.exports = (input) => {
    const hash = crypto.createHash('sha1');
    hash.update(typeof input === 'string' ? input : JSON.stringify(input));
    return base62.encode(hash.digest());   
}