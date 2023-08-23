const Kafka = require("node-rdkafka");
const cors = require("cors");
const express = require("express");
const bodyParser = require("body-parser");
const { configFromPath } = require("./util");
const app = express();
app.use(bodyParser.json());
app.use(cors());

app.get("/", (req, res) => {
  res.json("Successful response.");
});
app.post("/", (req, res) => {
  produceExample(req.body).catch((err) => {
    console.error(`Something went wrong:\n${err}`);
  });
  res.status(200).json("Success");
});

function createConfigMap(config) {
  if (config.hasOwnProperty("security.protocol")) {
    return {
      "bootstrap.servers": config["bootstrap.servers"],
      "sasl.username": config["sasl.username"],
      "sasl.password": config["sasl.password"],
      "security.protocol": config["security.protocol"],
      "sasl.mechanisms": config["sasl.mechanisms"],
      dr_msg_cb: true,
    };
  } else {
    return {
      "bootstrap.servers": config["bootstrap.servers"],
      dr_msg_cb: true,
    };
  }
}

function createProducer(config, onDeliveryReport) {
  const producer = new Kafka.Producer(createConfigMap(config));

  return new Promise((resolve, reject) => {
    producer
      .on("ready", () => resolve(producer))
      .on("delivery-report", onDeliveryReport)
      .on("event.error", (err) => {
        console.warn("event.error", err);
        reject(err);
      });
    producer.connect();
  });
}

async function produceExample(data) {
  let configPath = "getting-started.properties";
  const config = await configFromPath(configPath);

  let topic = "purchase";

  const producer = await createProducer(config, (err, report) => {
    if (err) {
      console.warn("Error producing", err);
    } else {
      const { topic, key, value } = report;
      let k = key.toString().padEnd(10, " ");
      console.log(
        `Produced event to topic ${topic}: key = ${k} value = ${value}`
      );
    }
  });

  let numEvents = data.length;
  for (let idx = 0; idx < numEvents; ++idx) {
    const key = data[idx].id;
    const value = Buffer.from(data[idx].value);

    producer.produce(topic, -1, value, key);
  }

  producer.flush(10000, () => {
    producer.disconnect();
  });
}

app.listen(3000, () => console.log("Producer server running on the port 3000"));
