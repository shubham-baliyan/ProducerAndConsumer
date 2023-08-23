const Kafka = require("node-rdkafka");
const express = require("express");
const cors = require("cors");

const bodyParser = require("body-parser");
const ws = require("ws");

const { configFromPath } = require("./util");
const app = express();
app.use(bodyParser.json());
app.use(cors());

const wsServer = new ws.Server({ noServer: true });
wsServer.on("connection", (socket) => {
  socket.on("message", (message) => {
    consumerExample(socket).catch((err) => {
      console.error(`Something went wrong:\n${err}`);
      process.exit(1);
    });
  });
});

//base route
app.get("/", (req, res) => {
  res.send("hello");
});

// creating a config
function createConfigMap(config) {
  if (config.hasOwnProperty("security.protocol")) {
    return {
      "bootstrap.servers": config["bootstrap.servers"],
      "sasl.username": config["sasl.username"],
      "sasl.password": config["sasl.password"],
      "security.protocol": config["security.protocol"],
      "sasl.mechanisms": config["sasl.mechanisms"],
      "group.id": "kafka-nodejs-getting-started",
    };
  } else {
    return {
      "bootstrap.servers": config["bootstrap.servers"],
      "group.id": "kafka-nodejs-getting-started",
    };
  }
}

function createConsumer(config, onData) {
  //kafka client
  const consumer = new Kafka.KafkaConsumer(createConfigMap(config), {
    "auto.offset.reset": "earliest",
  });

  return new Promise((resolve, reject) => {
    consumer.on("ready", () => resolve(consumer)).on("data", onData);

    consumer.connect();
  });
}

async function consumerExample(socket) {
  // getting the config from the below file
  let configPath = "getting-started.properties";
  const config = await configFromPath(configPath);

  //let seen = 0;
  // topic is set by default
  let topic = "purchase";

  const consumer = await createConsumer(config, ({ key, value }) => {
    let k = key.toString().padEnd(10, " ");
    socket.send(
      JSON.stringify({
        topic: `${topic}`,
        key: `${key}`,
        value: `${value}`,
        message: `Consumed event from topic ${topic}: key = ${k} value = ${value}`,
      })
    );
    console.log(
      `Consumed event from topic ${topic}: key = ${k} value = ${value}`
    );
  });
  // subscribing to this topic
  consumer.subscribe([topic]);
  //consuming the messages
  consumer.consume();

  //to close the connection with consumer after a while
  setTimeout(() => {
    console.log("\nDisconnecting consumer ...");

    consumer.disconnect();
    process.exit(0);
  }, 2000);
}

// starting the server
const server = app.listen(3001, () =>
  console.log("Websocket server is listening on port 3001")
);

//websocket
server.on("upgrade", (req, ws, h) => {
  wsServer.handleUpgrade(res, ws, h, (ws) => {
    wsServer.emit("connection", ws, res);
  });
});
