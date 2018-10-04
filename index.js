const r = require("rethinkdb");
const _ = require("highland");
const Path = require("path");
const fs = require("fs");

const run = async (query) => {
  const conn = await r.connect({
    host: "rethinkdb.mesopo.ai",
    password: "tigerwaterclockboulder",
    db: "blox"
  });

  return query.run(conn);
};

const each = async (query, callback) => {
  let cursor = await run(query);
  return cursor.eachAsync(callback);
};

let inputStream = (query) =>
  _((push) => {
    run(query)
      .then((cursor) => cursor.eachAsync((value) => push(null, value)))
      .then(() => push(null, _.nil));
  });

const writeToFile = (records) => {
  let first = records[0];
  if (!first) return;
  let fileName = `BLOX_${first.datetime_utc}.json`;
  let data = records.map(JSON.stringify).join("\n");
  fs.writeFileSync(Path.join("data", fileName), data);
};

inputStream(
  r
    .table("detections")
    .between(1526299107.8417606, r.maxval, { index: "frame_time" })
    .orderBy({ index: "frame_time" })
)
  .batch(10000)
  .each(writeToFile);
