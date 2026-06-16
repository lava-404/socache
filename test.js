const ws = new WebSocket("ws://localhost:3000/ws");

ws.onopen = () => {
    console.log("connected");

    ws.send(JSON.stringify({
      "jsonrpc": "2.0",
      "id": 1,
      "method": "accountSubscribe",
      "params": [
        "DtmVqUN2RAe7RHG4QJ4NoGRkMAkhFqchV32q6ibvu8aN",
        {
          "encoding": "jsonParsed",
          "commitment": "confirmed"
        }
      ]
    }   ));
};

ws.onmessage = (msg) => {
    console.log(msg.data);
};