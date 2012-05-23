#!/bin/bash
pkill -9 node
nohup node ./src/test/node/mock-feed.js ./src/test/node/sequence.mock 1> ./logs/mock-feed.log 2> ./logs/mock-feeds.err &
nohup node ./src/test/node/mock-api-server 1> ./logs/mock-api.log 2> ./logs/mock-api.err &
echo "End."
