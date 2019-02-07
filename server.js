const express = require('express');
const app = express();

var cfenv = require('cfenv');
var appEnv = cfenv.getAppEnv();
var emCreds = appEnv.getServiceCreds(process.env.EM_SERVICE);
var emCredsM = emCreds.messaging.filter(function (em) {
    return em.protocol == 'amqp10ws'
});

const options = {
  uri: emCredsM[0].uri,
  oa2:{
    endpoint: emCredsM[0].oa2.tokenendpoint,
    client: emCredsM[0].oa2.clientid,
    secret: emCredsM[0].oa2.clientsecret
  },
  data: {
    source: process.env.EM_SOURCE,
    payload: new Buffer.allocUnsafe(20),
    maxCount: 100,
    logCount: 10
  }
};

const { Client } = require('@sap/xb-msg-amqp-v100');
const client = new Client(options);
const stream = client.receiver('InputA').attach(options.data.source);

stream
  .on('data', (message) => {
    var payload = JSON.parse(message.payload.toString('utf8'));
    var eventPayload = payload.EVENT_PAYLOAD;
    console.log('Event: ' + JSON.stringify(eventPayload));
    message.done();
  });

client
  .on('connected',(destination, peerInfo) => {
    console.log('Connected!');
  })
  .on('assert', (error) => {
    console.log(error.message);
  })
  .on('error', (error) => {
    console.log(error);
  })
  .on('disconnected', (hadError, byBroker, statistics) => {
    console.log('Disconnected!');
  });

app.get('/', function (req, res) {
    res.type("text/html").status(200).send('<html><head></head><body><a href="/connect">Connect</a><br/><a href="/disconnect">Disconnect</a></body></html>');
});

app.get('/connect', function (req, res) {
    client.connect();
    res.status(200).send('Connected!');
});

app.get('/disconnect', function (req, res) {
    client.disconnect();
    res.status(200).send('Disconnected!');
});

const port = process.env.PORT || 3000;
app.listen(port, function () {
    console.info("Listening on port: " + port);
});
