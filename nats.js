module.exports = function(RED) {
  const nats = require('@nats-io/transport-node');


  /* utility functions */
  function connectToBroker(user, pass, address, port){
    let server = 'nats://' + user + ':' + pass + '@' + address + ':' + port + '/';
    return nats.connect({'servers': [server]});
  }
  
  function formatNatsError(err){
    return `${err.code ?? "unknown error code (internal node error)"}: ${err.input ?? "unknown user input (internal node error)"}`
  }

  /* subscription node */
  function NatsSubNode(config){
    RED.nodes.createNode(this, config);

    // node=this reference
    const node = this;

    // clear status
    node.status({});

    // extract config details out
    this.address = config.address;
    this.port = config.port;
    this.user = config.user;
    this.pass = config.pass;
    this.subject = config.subject;
    

    let natsConRef = null;
    
    // this.nc will be Promise<NatsConnection>
    this.nc = connectToBroker(config.user, config.pass, this.address, this.port);
    this.nc
      .then((natsConnection) => {
        // hacky reference
        natsConRef = natsConnection;
        //const subject = this.subject;
        node.status({"fill": "green", "shape": "dot", "text": "connected to broker"});

        // sid is an async iterator
        const sid = natsConnection.subscribe(this.subject);
        (async () => {
          for await (const msg of sid){
            node.send({"payload":msg.data, "subject": msg.subject});
          }
        })();
      })
      // notify node-red editor & user that nats broker connection failed
      .catch((err) => {
        let errMsg = formatNatsError(err);
        this.error(errMsg);
        node.status({"fill": "red", "shape": "ring", "text": errMsg});
      })
    
    // destructor for node
    this.on('close', function(){
      // clear status
      node.status({});
      if (natsConRef !== null){
        natsConRef.drain().then(() => {
          // happy flow; do nothing we chill
        }).catch(err => {
          // huh, we should log this.  Not sure how this would otherwise fail
          let errMsg = formatNatsError(err);
          node.warn("error closing nats con: " + errMsg);
        })
      }
    })
  }
  RED.nodes.registerType("nats-sub", NatsSubNode)

  function NatsPubNode(config){
    RED.nodes.createNode(this, config);
    
    // node=this reference
    const node = this;

    // clear status
    node.status({});

    // extract config details out
    this.address = config.address;
    this.port = config.port;
    this.user = config.user;
    this.pass = config.pass;


    // nats connection reference
    let natsConRef = null;


    // this.nc will be Promise<NatsConnection>
    this.nc = connectToBroker(config.user, config.pass, this.address, this.port);
    this.nc
      .then((natsConnection) => {
        // hacky reference update
        natsConRef = natsConnection;
        node.status({"fill": "green", "shape": "dot", "text": "connected to broker"});
      })
      // notify node-red editor & user that nats broker connection failed
      .catch((err) => {
        let errMsg = formatNatsError(err);
        this.error(errMsg);
        node.status({"fill": "red", "shape": "ring", "text": errMsg});
      })

    this.on('input', function(msg){
      this.subject = msg.payload.subject || config.subject;
      this.message = msg.payload.message || config.message;

      // so we essentially drop messages iff there's no 
      if (this.subject && this.message){
        if (natsConRef !== null) {
          natsConRef.publish(this.subject, this.message);
        }
      }

    })

    // destructor for node
    this.on('close', function(){
      // clear status
      node.status({});
      if (natsConRef !== null){
        natsConRef.drain().then(() => {
          // happy flow; do nothing we chill
        }).catch(err => {
          // huh, we should log this.  Not sure how this would otherwise fail
          let errMsg = formatNatsError(err);
          node.warn("error closing nats con: " + errMsg);
        })
      }
    })
  }

  RED.nodes.registerType("nats-pub",NatsPubNode);
}
