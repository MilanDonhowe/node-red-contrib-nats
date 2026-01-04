module.exports = function(RED) {
  const nats = require('@nats-io/transport-node');


  /* utility functions */
  function connectToBroker(user, pass, address, port){
    let server = 'nats://' + user + ':' + pass + '@' + address + ':' + port + '/';
    return nats.connect({'servers': [server]});
  }
  
  function formatNatsError(err){
    if (err.code && err.input){
      return `${err.code}: ${err.input}`
    }
    if (err.code){
      return `${err}`
    }
    return "unknown internal nats error"
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
    this.outputAsString = config.outputAsString;

    

    let natsConRef = null;
    
    // this.nc will be Promise<NatsConnection>
    this.nc = connectToBroker(config.user, config.pass, this.address, this.port);
    this.nc
      .then((natsConnection) => {
        // hacky reference
        natsConRef = natsConnection;
        
        node.status({"fill": "green", "shape": "dot", "text": `connected to broker`});

        // sid is an async iterator
        const sid = natsConnection.subscribe(this.subject);
        (async () => {
          for await (const msg of sid){
            // msg.data will default to a Buffer type
            let data = msg.data;
            // msg.string should be the UTF-8 encoded payload
            if (node.outputAsString){
              try {
                data = msg.string();
              } catch (error){
                // TODO: test this out
                // stole idea here from https://github.com/node-red/node-red/blob/6a75a084adc159de1be047e554b7463c306692a9/packages/node_modules/%40node-red/nodes/core/network/10-mqtt.js#L1323C27-L1323C62
                // since we don't accept input messages, not clear from docs on how to report errors to runtime
                node.error(RED._("nats.errors.utf-encoding"))
              }
            }
            node.send({"payload":data, "subject": msg.subject});
          }
          // at this point, our async iterator is closed so we clear node status here
          node.status({})
        })();
      })
      // notify node-red editor & user that nats broker connection failed
      .catch((err) => {
        let errMsg = formatNatsError(err);
        node.error(errMsg);
        node.status({"fill": "red", "shape": "ring", "text": errMsg});
      })
    
    // destructor for node
    node.on('close', function(done){
      if (natsConRef !== null){
        natsConRef.drain().then(() => {
          // happy flow; do nothing we chill
          done();
        }).catch(err => {
          // huh, we should log this.  Not sure how this would otherwise fail
          let errMsg = formatNatsError(err);
          node.warn("error closing nats con: " + errMsg);
          done();
        })
      } else {
        // IMPORTANT! if no connection exists immediately signal we're done to node-red
        done();
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

      // so we essentially drop messages if there's no active connection
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
